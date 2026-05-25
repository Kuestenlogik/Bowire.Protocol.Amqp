// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// Broker-free unit coverage for <see cref="AmqpMockEmitter"/>. The
/// wire-level "does the publish hit RabbitMQ?" path is exercised in
/// the Testcontainers-backed integration suite; here we cover the
/// payload-decoding + step-filter logic that's the same regardless
/// of broker reachability.
/// </summary>
public sealed class AmqpMockEmitterTests
{
    [Fact]
    public async Task CanEmit_TrueWhenRecordingHasAmqpStep()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep { Protocol = "rest" },
                new BowireRecordingStep { Protocol = "amqp", ServerUrl = "amqp://localhost:5672/" },
            },
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_FalseWhenRecordingHasOnlyAmqp10Steps()
    {
        // 0.9.1-only emitter today — 1.0 steps don't claim the recording.
        // A mixed recording with at least one 0.9.1 step still claims
        // it; this case has none.
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep { Protocol = "amqp", ServerUrl = "amqp1://broker/" },
            },
        };
        Assert.False(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_FalseWhenRecordingHasNoAmqpStep()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep { Protocol = "rest" },
                new BowireRecordingStep { Protocol = "kafka" },
            },
        };
        Assert.False(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task Id_is_amqp()
    {
        await using var emitter = new AmqpMockEmitter();
        Assert.Equal("amqp", emitter.Id);
    }

    [Fact]
    public void DecodePayload_prefers_ResponseBinary_over_Body()
    {
        var step = new BowireRecordingStep
        {
            Id = "s1",
            Protocol = "amqp",
            Body = "fallback-text",
            ResponseBinary = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("real-bytes")),
        };
        var bytes = AmqpMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.NotNull(bytes);
        Assert.Equal("real-bytes", System.Text.Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void DecodePayload_falls_back_to_Body_when_ResponseBinary_missing()
    {
        var step = new BowireRecordingStep
        {
            Id = "s2",
            Protocol = "amqp",
            Body = "hello",
        };
        var bytes = AmqpMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.NotNull(bytes);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void DecodePayload_falls_back_to_Body_when_ResponseBinary_is_not_valid_base64()
    {
        // Junk base64 → emitter logs a warning + falls back to Body
        // rather than throwing. Recording produced by an older capture
        // shouldn't poison the replay loop.
        var step = new BowireRecordingStep
        {
            Id = "s3",
            Protocol = "amqp",
            Body = "fallback",
            ResponseBinary = "not===base64===",
        };
        var bytes = AmqpMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.NotNull(bytes);
        Assert.Equal("fallback", System.Text.Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void DecodePayload_returns_null_when_both_payload_sources_missing()
    {
        var step = new BowireRecordingStep { Id = "empty", Protocol = "amqp" };
        var bytes = AmqpMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.Null(bytes);
    }

    // -------- CanEmit edge cases --------

    [Fact]
    public async Task CanEmit_AmqpStepWithoutServerUrl_StillClaims()
    {
        // IsAmqp091Step accepts null ServerUrl — the broker URL defaults
        // to amqp://localhost:5672/ at StartAsync time, so a recording
        // with a bare-protocol step is still ours.
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "amqp", ServerUrl = null } },
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_AmqpsScheme_IsAlsoClaimed()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "amqp", ServerUrl = "amqps://broker:5671/" } },
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_ProtocolCasingIsIgnored()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "AMQP", ServerUrl = "amqp://broker/" } },
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public void CanEmit_NullRecording_Throws()
    {
        var emitter = new AmqpMockEmitter();
        Assert.Throws<ArgumentNullException>(() => emitter.CanEmit(null!));
    }

    // -------- StartAsync early-return paths --------

    [Fact]
    public async Task StartAsync_EmptyRecording_NoBrokerContact()
    {
        // No amqp/0.9.1 steps → StartAsync bails out before
        // ConnectionFactory.CreateConnectionAsync runs. The test
        // succeeds simply by completing without a broker.
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording { Steps = { new BowireRecordingStep { Protocol = "rest" } } };

        await emitter.StartAsync(
            rec, new MockEmitterOptions(), NullLogger.Instance, CancellationToken.None);
        // Nothing to assert beyond "no throw, no hang" — the early
        // return is the entire contract for this branch.
    }

    [Fact]
    public async Task StartAsync_RecordingWithOnlyAmqp10Steps_BailsOut()
    {
        // After filtering 1.0 steps the list is empty; emitter logs
        // the skip-warning *and* returns without opening a connection
        // because the steps.Count == 0 check fires immediately.
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "amqp", ServerUrl = "amqp1://broker/" } },
        };

        await emitter.StartAsync(
            rec, new MockEmitterOptions(), NullLogger.Instance, CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_NullRecording_Throws()
    {
        await using var emitter = new AmqpMockEmitter();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => emitter.StartAsync(null!, new MockEmitterOptions(), NullLogger.Instance, CancellationToken.None));
    }

    [Fact]
    public async Task StartAsync_NullOptions_Throws()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => emitter.StartAsync(rec, null!, NullLogger.Instance, CancellationToken.None));
    }

    [Fact]
    public async Task StartAsync_NullLogger_Throws()
    {
        await using var emitter = new AmqpMockEmitter();
        var rec = new BowireRecording();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => emitter.StartAsync(rec, new MockEmitterOptions(), null!, CancellationToken.None));
    }

    // -------- DisposeAsync paths --------

    [Fact]
    public async Task DisposeAsync_BeforeStart_IsNoOp()
    {
        var emitter = new AmqpMockEmitter();
        await emitter.DisposeAsync();
        // Second dispose stays a no-op — the _disposed guard fires.
        await emitter.DisposeAsync();
    }
}
