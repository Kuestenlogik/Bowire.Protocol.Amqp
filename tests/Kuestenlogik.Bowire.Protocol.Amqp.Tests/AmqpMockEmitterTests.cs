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
}
