// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using RabbitMQ.Client;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// Coverage for <see cref="AmqpSecurityConfig"/> — the mTLS + SASL
/// marker translator. Exercises the pure parse / mapping paths
/// without standing up a real broker: hand-built metadata dicts
/// flow through ApplyV091 / ApplyV10 and the resulting factory
/// state is inspected directly.
/// </summary>
public sealed class AmqpSecurityConfigTests
{
    // ------------- SaslConfig.TryParse -------------

    [Fact]
    public void SaslConfig_TryParse_ReturnsNullForInvalidJson()
    {
        Assert.Null(AmqpSecurityConfig.SaslConfig.TryParse("{ not json"));
    }

    [Fact]
    public void SaslConfig_TryParse_ReturnsNullForNonObjectJson()
    {
        Assert.Null(AmqpSecurityConfig.SaslConfig.TryParse("\"just a string\""));
        Assert.Null(AmqpSecurityConfig.SaslConfig.TryParse("[1,2,3]"));
    }

    [Fact]
    public void SaslConfig_TryParse_DefaultsToPlainWhenMechanismOmitted()
    {
        var cfg = AmqpSecurityConfig.SaslConfig.TryParse("""{"username":"u","password":"p"}""");
        Assert.NotNull(cfg);
        Assert.Equal("PLAIN", cfg!.Mechanism);
        Assert.Equal("u", cfg.Username);
        Assert.Equal("p", cfg.Password);
    }

    [Fact]
    public void SaslConfig_TryParse_UppercasesMechanism()
    {
        var cfg = AmqpSecurityConfig.SaslConfig.TryParse("""{"mechanism":"external","username":"u"}""");
        Assert.NotNull(cfg);
        Assert.Equal("EXTERNAL", cfg!.Mechanism);
    }

    [Fact]
    public void SaslConfig_TryParse_TreatsMissingUsernameAsEmpty()
    {
        var cfg = AmqpSecurityConfig.SaslConfig.TryParse("""{"mechanism":"PLAIN"}""");
        Assert.NotNull(cfg);
        Assert.Equal(string.Empty, cfg!.Username);
        Assert.Equal(string.Empty, cfg.Password);
    }

    [Fact]
    public void SaslConfig_TryParse_IgnoresNonStringFields()
    {
        // Numeric username should be treated as absent (Get returns null
        // when the JsonValueKind isn't String), so the field defaults.
        var cfg = AmqpSecurityConfig.SaslConfig.TryParse(
            """{"mechanism":"PLAIN","username":42}""");
        Assert.NotNull(cfg);
        Assert.Equal(string.Empty, cfg!.Username);
    }

    // ------------- ApplyV091: no-op paths -------------

    [Fact]
    public void ApplyV091_ReturnsNullWhenMetadataIsNull()
    {
        var factory = new ConnectionFactory();
        Assert.Null(AmqpSecurityConfig.ApplyV091(factory, null));
    }

    [Fact]
    public void ApplyV091_ReturnsSameDictWhenEmpty()
    {
        var empty = new Dictionary<string, string>();
        var factory = new ConnectionFactory();
        var result = AmqpSecurityConfig.ApplyV091(factory, empty);
        Assert.Same(empty, result);
    }

    [Fact]
    public void ApplyV091_PassesThroughForeignKeys_Unchanged()
    {
        var meta = new Dictionary<string, string>
        {
            ["x-custom-header"] = "value",
            ["routingKey"] = "orders",
        };
        var factory = new ConnectionFactory();
        var result = AmqpSecurityConfig.ApplyV091(factory, meta);
        Assert.NotNull(result);
        Assert.Equal("value", result!["x-custom-header"]);
        Assert.Equal("orders", result["routingKey"]);
    }

    // ------------- ApplyV091: SASL marker -------------

    [Fact]
    public void ApplyV091_PlainSasl_SetsUsernameAndPassword()
    {
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] =
                """{"mechanism":"PLAIN","username":"alice","password":"s3cr3t"}""",
        };
        var factory = new ConnectionFactory();
        var result = AmqpSecurityConfig.ApplyV091(factory, meta);

        Assert.Equal("alice", factory.UserName);
        Assert.Equal("s3cr3t", factory.Password);
        Assert.NotNull(factory.AuthMechanisms);
        var mechs = factory.AuthMechanisms.ToList();
        Assert.Single(mechs);
        Assert.IsType<PlainMechanismFactory>(mechs[0]);
        // marker stripped from the returned dict.
        Assert.False(result!.ContainsKey(AmqpSecurityConfig.SaslMarkerKey));
    }

    [Fact]
    public void ApplyV091_ExternalSasl_UsesExternalMechanism()
    {
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = """{"mechanism":"EXTERNAL"}""",
        };
        var factory = new ConnectionFactory();
        AmqpSecurityConfig.ApplyV091(factory, meta);

        Assert.IsType<ExternalMechanismFactory>(factory.AuthMechanisms!.ToList()[0]);
    }

    [Fact]
    public void ApplyV091_AnonymousSasl_UsesPlainMechanism()
    {
        // ANONYMOUS maps onto PlainMechanismFactory in the 0.9.1 path
        // (broker accepts blank PLAIN as anonymous).
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = """{"mechanism":"ANONYMOUS"}""",
        };
        var factory = new ConnectionFactory();
        AmqpSecurityConfig.ApplyV091(factory, meta);

        Assert.IsType<PlainMechanismFactory>(factory.AuthMechanisms!.ToList()[0]);
    }

    [Fact]
    public void ApplyV091_InvalidSaslJson_IsIgnored()
    {
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = "not json at all",
            ["routing"] = "kept",
        };
        var factory = new ConnectionFactory();
        var result = AmqpSecurityConfig.ApplyV091(factory, meta);

        // Default factory values stay (null AuthMechanisms / "guest"
        // username). The bad marker is still stripped from the result.
        Assert.False(result!.ContainsKey(AmqpSecurityConfig.SaslMarkerKey));
        Assert.Equal("kept", result["routing"]);
    }

    // ------------- ApplyV091: mTLS marker -------------

    [Fact]
    public void ApplyV091_MtlsMarker_EnablesSsl()
    {
        var meta = BuildMtlsMeta(allowSelfSigned: false);
        var factory = new ConnectionFactory { HostName = "broker.example.com" };

        var result = AmqpSecurityConfig.ApplyV091(factory, meta);

        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("broker.example.com", factory.Ssl.ServerName);
        Assert.NotNull(factory.Ssl.Certs);
        Assert.Single(factory.Ssl.Certs!);
        Assert.Equal(System.Net.Security.SslPolicyErrors.None, factory.Ssl.AcceptablePolicyErrors);
        Assert.False(result!.ContainsKey("__bowireMtls__"));
    }

    [Fact]
    public void ApplyV091_MtlsMarker_AllowSelfSigned_RelaxesPolicy()
    {
        var meta = BuildMtlsMeta(allowSelfSigned: true);
        var factory = new ConnectionFactory();

        AmqpSecurityConfig.ApplyV091(factory, meta);

        Assert.True(factory.Ssl.Enabled);
        Assert.True(factory.Ssl.AcceptablePolicyErrors.HasFlag(
            System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.True(factory.Ssl.AcceptablePolicyErrors.HasFlag(
            System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch));
    }

    // ------------- ApplyV10: parallel surface -------------

    [Fact]
    public void ApplyV10_ReturnsNullWhenMetadataIsNull()
    {
        var factory = new global::Amqp.ConnectionFactory();
        Assert.Null(AmqpSecurityConfig.ApplyV10(factory, null));
    }

    [Fact]
    public void ApplyV10_PassesThroughForeignKeys()
    {
        var meta = new Dictionary<string, string> { ["custom"] = "v" };
        var factory = new global::Amqp.ConnectionFactory();
        var result = AmqpSecurityConfig.ApplyV10(factory, meta);
        Assert.Equal("v", result!["custom"]);
    }

    [Fact]
    public void ApplyV10_MtlsMarker_AddsClientCert()
    {
        var meta = BuildMtlsMeta(allowSelfSigned: false);
        var factory = new global::Amqp.ConnectionFactory();

        AmqpSecurityConfig.ApplyV10(factory, meta);

        Assert.NotNull(factory.SSL.ClientCertificates);
        Assert.True(factory.SSL.ClientCertificates.Count >= 1);
    }

    [Fact]
    public void ApplyV10_MtlsMarker_AllowSelfSigned_SetsCallback()
    {
        var meta = BuildMtlsMeta(allowSelfSigned: true);
        var factory = new global::Amqp.ConnectionFactory();

        AmqpSecurityConfig.ApplyV10(factory, meta);

        Assert.NotNull(factory.SSL.RemoteCertificateValidationCallback);
        // The callback returns true unconditionally.
        var ok = factory.SSL.RemoteCertificateValidationCallback!(
            this, null, null, System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors);
        Assert.True(ok);
    }

    [Fact]
    public void ApplyV10_ExternalSasl_SetsExternalProfile()
    {
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = """{"mechanism":"EXTERNAL"}""",
        };
        var factory = new global::Amqp.ConnectionFactory();
        AmqpSecurityConfig.ApplyV10(factory, meta);

        // AMQPNetLite's SaslProfile.External returns a fresh
        // SaslNoActionProfile each call; identity / type-name aren't
        // useful discriminators. The mechanism is on a non-public
        // Mechanism property — reach it via reflection.
        Assert.NotNull(factory.SASL.Profile);
        Assert.Equal("EXTERNAL", GetSaslMechanism(factory.SASL.Profile));
    }

    [Fact]
    public void ApplyV10_AnonymousSasl_SetsAnonymousProfile()
    {
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = """{"mechanism":"ANONYMOUS"}""",
        };
        var factory = new global::Amqp.ConnectionFactory();
        AmqpSecurityConfig.ApplyV10(factory, meta);

        Assert.NotNull(factory.SASL.Profile);
        Assert.Equal("ANONYMOUS", GetSaslMechanism(factory.SASL.Profile));
    }

    [Fact]
    public void ApplyV10_PlainSasl_LeavesProfileDefault()
    {
        // PLAIN doesn't set a profile — the library defaults to PLAIN
        // when the Address carries user/pass. The strip-step still runs.
        var meta = new Dictionary<string, string>
        {
            [AmqpSecurityConfig.SaslMarkerKey] = """{"mechanism":"PLAIN","username":"u","password":"p"}""",
        };
        var factory = new global::Amqp.ConnectionFactory();
        var before = factory.SASL.Profile;

        var result = AmqpSecurityConfig.ApplyV10(factory, meta);

        Assert.Same(before, factory.SASL.Profile);
        Assert.False(result!.ContainsKey(AmqpSecurityConfig.SaslMarkerKey));
    }

    // ------------- helpers -------------

    /// <summary>
    /// Reach into AMQPNetLite's SaslProfile / SaslNoActionProfile to
    /// pull the wire mechanism string. The Mechanism property is
    /// public on SaslNoActionProfile but on the base SaslProfile it's
    /// protected — reflecting over a 'Mechanism' member on whichever
    /// type the profile actually is handles both shapes.
    /// </summary>
    private static string GetSaslMechanism(object profile)
    {
        var t = profile.GetType();
        var prop = t.GetProperty("Mechanism",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic
            | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(prop);
        return prop!.GetValue(profile)?.ToString() ?? "";
    }


    /// <summary>
    /// Build a metadata dict with a valid mTLS marker. Generates a
    /// throwaway self-signed RSA cert + key on the fly so the test
    /// doesn't depend on a fixture file. The cert never sees a real
    /// broker — it only has to round-trip through
    /// X509Certificate2.CreateFromPem cleanly.
    /// </summary>
    private static Dictionary<string, string> BuildMtlsMeta(bool allowSelfSigned)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(
            "CN=bowire-amqp-tests", rsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        var certPem = "-----BEGIN CERTIFICATE-----\n"
                    + Convert.ToBase64String(cert.RawData,
                          Base64FormattingOptions.InsertLineBreaks)
                    + "\n-----END CERTIFICATE-----\n";
        var keyPem = "-----BEGIN PRIVATE KEY-----\n"
                   + Convert.ToBase64String(rsa.ExportPkcs8PrivateKey(),
                         Base64FormattingOptions.InsertLineBreaks)
                   + "\n-----END PRIVATE KEY-----\n";

        var payload = JsonSerializer.Serialize(new
        {
            certificate = certPem,
            privateKey = keyPem,
            allowSelfSigned,
        });
        return new Dictionary<string, string>
        {
            ["__bowireMtls__"] = payload,
        };
    }
}
