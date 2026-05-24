// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kuestenlogik.Bowire.Auth;
using RabbitMQ.Client;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Translates Bowire auth markers off the request-metadata bag into AMQP
/// security knobs. Two markers flow in:
/// <list type="bullet">
/// <item>
///   <c>__bowireMtls__</c> — shared with REST/gRPC/Kafka/SignalR (decoded
///   via <see cref="MtlsConfig.TryParseFromMetadata"/>). PEM cert + key +
///   optional CA / passphrase / allow-self-signed. Maps onto
///   <see cref="ConnectionFactory.Ssl"/> for 0.9.1 and the
///   <c>ConnectionFactory.SSL.ClientCertificates</c> +
///   <c>RemoteCertificateValidationCallback</c> pair for 1.0.
/// </item>
/// <item>
///   <c>__bowireAmqpSasl__</c> — AMQP-specific JSON
///   <c>{ mechanism, username, password }</c>. Mechanism is one of
///   <c>PLAIN</c>, <c>EXTERNAL</c>, <c>ANONYMOUS</c>. The 0.9.1 wire
///   sets <see cref="ConnectionFactory.AuthMechanisms"/> accordingly;
///   the 1.0 wire's AMQPNetLite picks the SASL profile from the
///   <see cref="global::Amqp.ConnectionFactory.SASL"/> options.
/// </item>
/// </list>
/// </summary>
/// <remarks>
/// Discovery has no metadata channel today (IBowireProtocol.DiscoverAsync
/// is metadata-less) — these knobs only kick in at invoke / stream time,
/// matching the Kafka plugin's contract. Discovery against a TLS-only
/// broker still works as long as the workbench's discovery probe hits
/// the management API on an unauthenticated port, or the user opts in
/// to the basic guest/guest path the URL exposes.
/// </remarks>
internal static class AmqpSecurityConfig
{
    /// <summary>Magic metadata key for SASL credentials.</summary>
    public const string SaslMarkerKey = "__bowireAmqpSasl__";

    /// <summary>
    /// Apply mTLS + SASL markers to a 0.9.1 RabbitMQ.Client connection
    /// factory. Returns the metadata dict with both markers stripped so
    /// the caller can forward what's left as broker headers without
    /// leaking secrets.
    /// </summary>
    public static Dictionary<string, string>? ApplyV091(
        ConnectionFactory factory, Dictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0) return metadata;

        var mtls = MtlsConfig.TryParseFromMetadata(metadata);
        if (mtls is not null)
        {
            // RabbitMQ.Client owns the SslOption struct; we layer the
            // PEM material onto it as an X509Certificate2 collection.
            // RabbitMQ.Client trips on a SslOption that has Enabled=false
            // and ClientCertificates set, so flip Enabled too.
            using var leaf = mtls.AllowSelfSigned ? null : default(X509Certificate2);
            var cert = X509Certificate2.CreateFromPem(mtls.CertificatePem, mtls.PrivateKeyPem);
            if (!string.IsNullOrEmpty(mtls.Passphrase))
            {
                // X509Certificate2.CreateFromPem doesn't take a passphrase
                // directly; for encrypted PEM the helper expects the key
                // already decrypted. Bowire's UI decrypts before shipping,
                // so passphrase here is for symmetry only (no-op).
            }
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = factory.HostName,
                Certs = new X509Certificate2Collection { cert },
                AcceptablePolicyErrors = mtls.AllowSelfSigned
                    ? System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors
                      | System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch
                    : System.Net.Security.SslPolicyErrors.None,
            };
        }

        if (metadata.TryGetValue(SaslMarkerKey, out var saslJson) &&
            SaslConfig.TryParse(saslJson) is { } sasl)
        {
            // 0.9.1 mechanism plumbing is via AuthMechanisms — a list of
            // factories, ordered by preference. For PLAIN/EXTERNAL/
            // ANONYMOUS the built-in factories cover everything we
            // expose in the UI.
            factory.AuthMechanisms = sasl.Mechanism switch
            {
                "EXTERNAL" => [new ExternalMechanismFactory()],
                "ANONYMOUS" => [new PlainMechanismFactory()],
                _ => [new PlainMechanismFactory()],
            };
            if (!string.IsNullOrEmpty(sasl.Username)) factory.UserName = sasl.Username;
            if (!string.IsNullOrEmpty(sasl.Password)) factory.Password = sasl.Password;
        }

        return Strip(metadata);
    }

    /// <summary>
    /// Apply markers to an AMQPNetLite 1.0 connection factory. Same
    /// contract as the 0.9.1 path — returns the stripped metadata dict.
    /// </summary>
    public static Dictionary<string, string>? ApplyV10(
        global::Amqp.ConnectionFactory factory, Dictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0) return metadata;

        var mtls = MtlsConfig.TryParseFromMetadata(metadata);
        if (mtls is not null)
        {
            var cert = X509Certificate2.CreateFromPem(mtls.CertificatePem, mtls.PrivateKeyPem);
            factory.SSL.ClientCertificates.Add(cert);
            if (mtls.AllowSelfSigned)
            {
                // Operator explicitly opted into "allow self-signed" in
                // the workbench auth dialog — same UI contract as the REST
                // / gRPC / Kafka plugins. Suppressing the analyser here
                // is the documented behaviour, not an oversight.
#pragma warning disable CA5359
                factory.SSL.RemoteCertificateValidationCallback =
                    (_, _, _, _) => true;
#pragma warning restore CA5359
            }
        }

        if (metadata.TryGetValue(SaslMarkerKey, out var saslJson) &&
            SaslConfig.TryParse(saslJson) is { } sasl)
        {
            // AMQPNetLite ships SaslProfile.External / .Anonymous as
            // public statics. PLAIN is not addressable as a public
            // profile (SaslPlainProfile is internal in the netstandard
            // build); the convention there is "carry user:pass on the
            // Address itself" — the caller composes the address from
            // sasl.Username / sasl.Password (which we hand back through
            // the strip-metadata return so InvokeV10/ReceiveV10 can
            // override the URL credentials when the marker carries
            // fresher ones).
            if (sasl.Mechanism == "EXTERNAL")
            {
                factory.SASL.Profile = global::Amqp.Sasl.SaslProfile.External;
            }
            else if (sasl.Mechanism == "ANONYMOUS")
            {
                factory.SASL.Profile = global::Amqp.Sasl.SaslProfile.Anonymous;
            }
            // PLAIN: no factory.SASL.Profile assignment needed — the
            // library defaults to PLAIN whenever the Address carries
            // user/pass, and InvokeV10/ReceiveV10 already build the
            // Address from endpoint + sasl override (see those
            // call-sites). The strip-marker step still runs below
            // so the credentials don't leak as AMQP application
            // properties.
        }

        return Strip(metadata);
    }

    private static Dictionary<string, string> Strip(Dictionary<string, string> metadata)
    {
        var copy = new Dictionary<string, string>(metadata.Count, StringComparer.Ordinal);
        foreach (var (k, v) in metadata)
        {
            if (string.Equals(k, MtlsConfig.MtlsMarkerKey, StringComparison.Ordinal)) continue;
            if (string.Equals(k, SaslMarkerKey, StringComparison.Ordinal)) continue;
            copy[k] = v;
        }
        return copy;
    }

    /// <summary>
    /// Parsed SASL credentials carried inline via <see cref="SaslMarkerKey"/>.
    /// </summary>
    internal sealed record SaslConfig(string Mechanism, string Username, string Password)
    {
        public static SaslConfig? TryParse(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) return null;

                string? Get(string name) =>
                    root.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
                        ? p.GetString()
                        : null;

                var mechanism = (Get("mechanism") ?? "PLAIN").ToUpperInvariant();
                var username = Get("username") ?? string.Empty;
                var password = Get("password") ?? string.Empty;

                return new SaslConfig(mechanism, username, password);
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }
}
