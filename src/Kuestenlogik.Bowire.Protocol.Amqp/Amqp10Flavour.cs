// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Which broker an AMQP 1.0 endpoint is, as far as discovery is
/// concerned. AMQP 1.0 itself has no way to list what a broker holds;
/// the two brokers Bowire meets most both have one, and they are not the
/// same one.
/// </summary>
internal enum Amqp10Flavour
{
    /// <summary>
    /// Decide from the endpoint: a Service Bus host by its name, otherwise
    /// try Artemis' management address and fall back to the bare
    /// <c>Broker</c> service when nothing answers.
    /// </summary>
    Auto,

    /// <summary>ActiveMQ Artemis — management over AMQP itself, on <c>activemq.management</c>.</summary>
    Artemis,

    /// <summary>Azure Service Bus — the namespace's ATOM management feed over HTTPS, signed with the SAS key.</summary>
    ServiceBus,

    /// <summary>No discovery: the synthetic <c>Broker</c> service with generic send/receive.</summary>
    None,
}

internal static class Amqp10Flavours
{
    /// <summary>The setting / query value → flavour. Unknown or empty is <see cref="Amqp10Flavour.Auto"/>.</summary>
    public static Amqp10Flavour Parse(string? value) => value?.Trim().ToUpperInvariant() switch
    {
        "ARTEMIS" => Amqp10Flavour.Artemis,
        "SERVICEBUS" or "SERVICE-BUS" or "AZURE" => Amqp10Flavour.ServiceBus,
        "NONE" or "BROKER" => Amqp10Flavour.None,
        _ => Amqp10Flavour.Auto,
    };

    /// <summary>
    /// A Service Bus namespace by its host — the public cloud and the
    /// sovereign ones. The namespace is the first label; everything after
    /// it is the cloud.
    /// </summary>
    public static bool IsServiceBusHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host)) return false;
        return host.EndsWith(".servicebus.windows.net", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".servicebus.usgovcloudapi.net", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".servicebus.chinacloudapi.cn", StringComparison.OrdinalIgnoreCase)
            || host.EndsWith(".servicebus.cloudapi.de", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>What <see cref="Amqp10Flavour.Auto"/> resolves to before any probe: Service Bus by host, otherwise Artemis first.</summary>
    public static Amqp10Flavour Resolve(Amqp10Flavour requested, string? host)
        => requested == Amqp10Flavour.Auto
            ? (IsServiceBusHost(host) ? Amqp10Flavour.ServiceBus : Amqp10Flavour.Artemis)
            : requested;
}
