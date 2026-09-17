// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Amqp;
using Amqp.Framing;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>One Artemis address with the queues bound to it.</summary>
internal sealed record ArtemisAddress(
    string Name,
    IReadOnlyList<string> RoutingTypes,
    bool Internal,
    bool Temporary,
    bool AutoCreated,
    List<ArtemisQueue> Queues);

/// <summary>One Artemis queue, as <c>listQueues</c> reports it.</summary>
internal sealed record ArtemisQueue(
    string Name,
    string Address,
    string RoutingType,
    bool Temporary,
    bool Internal,
    long MessageCount,
    int ConsumerCount);

/// <summary>
/// ActiveMQ Artemis' management API, spoken over the AMQP connection
/// itself: a message to the <c>activemq.management</c> address whose
/// application properties name the resource (<c>broker</c>) and the
/// operation, whose body is the JSON array of parameters, and whose reply
/// — to a temporary queue the broker creates for a dynamic receiver —
/// carries <c>_AMQ_OperationSucceeded</c> and the JSON array of results.
/// The same socket, the same credentials, no second port: what the
/// Jolokia console on 8161 would need a separate login for, the broker
/// answers to anyone it already let in.
/// </summary>
/// <remarks>
/// <para>
/// Two operations cover the surface: <c>listAddresses</c> and
/// <c>listQueues</c>, each with the console's own paging arguments
/// (an options document, page number, page size). They return a JSON
/// document as a string inside the result array —
/// <c>{"data":[…],"count":N}</c> — with every attribute a string, the way
/// the console reads them. Verified against Artemis 2.44.
/// </para>
/// <para>
/// A broker that is not Artemis does not have the address; the send is
/// refused or nothing answers within the discovery timeout, and the
/// caller treats either as "not Artemis".
/// </para>
/// </remarks>
internal static class ArtemisManagement
{
    /// <summary>The management address every Artemis broker serves.</summary>
    public const string ManagementAddress = "activemq.management";

    private const string ResourceNameKey = "_AMQ_ResourceName";
    private const string OperationNameKey = "_AMQ_OperationName";
    private const string OperationSucceededKey = "_AMQ_OperationSucceeded";
    private const int PageSize = 500;

    private static readonly JsonSerializerOptions s_json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// The broker's addresses with their queues, or null when the
    /// endpoint is not an Artemis broker (nothing at the management
    /// address, or a reply that is not one).
    /// </summary>
    public static async Task<List<ArtemisAddress>?> ListAddressesAsync(
        Connection connection, TimeSpan timeout, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var session = new Session(connection);
        ReceiverLink? receiver = null;
        SenderLink? sender = null;
        try
        {
            // The reply lands on a temporary queue: a receiver attached
            // with a dynamic source, whose address the broker assigns on
            // attach. Artemis deletes it with the link.
            string? replyTo = null;
            var attached = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiver = new ReceiverLink(session, "bowire-mgmt-reply-" + Guid.NewGuid().ToString("N"),
                new Source { Dynamic = true },
                (_, attach) =>
                {
                    replyTo = (attach.Source as Source)?.Address;
                    attached.TrySetResult(true);
                });
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(timeout);
            await attached.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
            if (string.IsNullOrEmpty(replyTo)) return null;
            receiver.SetCredit(16);

            sender = new SenderLink(session, "bowire-mgmt-" + Guid.NewGuid().ToString("N"), ManagementAddress);

            var addressesJson = await InvokeAsync(sender, receiver, replyTo, "broker", "listAddresses", PagedArgs(), timeout, ct).ConfigureAwait(false);
            if (addressesJson is null) return null;
            var queuesJson = await InvokeAsync(sender, receiver, replyTo, "broker", "listQueues", PagedArgs(), timeout, ct).ConfigureAwait(false);
            if (queuesJson is null) return null;

            return Compose(ParseAddresses(addressesJson), ParseQueues(queuesJson));
        }
        catch (AmqpException)
        {
            // The management address refused — a broker that is not Artemis.
            return null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Nothing answered within the discovery timeout.
            return null;
        }
        finally
        {
            if (sender is not null) await sender.CloseAsync().ConfigureAwait(false);
            if (receiver is not null) await receiver.CloseAsync().ConfigureAwait(false);
            await session.CloseAsync().ConfigureAwait(false);
        }
    }

    // The console's list arguments: an options document with no filter,
    // sorted by name, the first page of PageSize. A broker with more than
    // PageSize addresses reports `count` above that; the first page is
    // what a sidebar can show anyway.
    private static string PagedArgs()
        => "[\"{\\\"field\\\":\\\"\\\",\\\"operation\\\":\\\"\\\",\\\"value\\\":\\\"\\\",\\\"sortColumn\\\":\\\"name\\\",\\\"sortOrder\\\":\\\"asc\\\"}\",1," + PageSize + "]";

    /// <summary>
    /// One management request/reply. Returns the first element of the
    /// result array as a string (both list operations return a JSON
    /// document as one), or null when the broker said the operation
    /// failed or the reply is not shaped like one.
    /// </summary>
    private static async Task<string?> InvokeAsync(
        SenderLink sender, ReceiverLink receiver, string replyTo,
        string resource, string operation, string parametersJson,
        TimeSpan timeout, CancellationToken ct)
    {
        using var request = new Message(parametersJson)
        {
            Properties = new Properties { ReplyTo = replyTo, MessageId = Guid.NewGuid().ToString("N") },
            ApplicationProperties = new ApplicationProperties(),
        };
        request.ApplicationProperties[ResourceNameKey] = resource;
        request.ApplicationProperties[OperationNameKey] = operation;
        await sender.SendAsync(request).WaitAsync(timeout, ct).ConfigureAwait(false);

        var reply = await receiver.ReceiveAsync(timeout).WaitAsync(ct).ConfigureAwait(false);
        if (reply is null) return null;
        receiver.Accept(reply);

        if (reply.ApplicationProperties?[OperationSucceededKey] is bool succeeded && !succeeded) return null;
        var body = reply.Body as string;
        if (string.IsNullOrEmpty(body)) return null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Array || doc.RootElement.GetArrayLength() == 0) return null;
            var first = doc.RootElement[0];
            return first.ValueKind == JsonValueKind.String ? first.GetString() : first.GetRawText();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // ---- the list documents ----

    private sealed record ListDocument(List<Dictionary<string, string>>? Data, int? Count);

    internal static List<ArtemisAddress> ParseAddresses(string listAddressesJson)
    {
        var doc = JsonSerializer.Deserialize<ListDocument>(listAddressesJson, s_json);
        var result = new List<ArtemisAddress>();
        foreach (var row in doc?.Data ?? [])
        {
            var name = Get(row, "name");
            if (string.IsNullOrEmpty(name)) continue;
            result.Add(new ArtemisAddress(
                Name: name,
                RoutingTypes: ParseRoutingTypes(Get(row, "routingTypes")),
                Internal: Flag(row, "internal"),
                Temporary: Flag(row, "temporary"),
                AutoCreated: Flag(row, "autoCreated"),
                Queues: []));
        }
        return result;
    }

    internal static List<ArtemisQueue> ParseQueues(string listQueuesJson)
    {
        var doc = JsonSerializer.Deserialize<ListDocument>(listQueuesJson, s_json);
        var result = new List<ArtemisQueue>();
        foreach (var row in doc?.Data ?? [])
        {
            var name = Get(row, "name");
            if (string.IsNullOrEmpty(name)) continue;
            result.Add(new ArtemisQueue(
                Name: name,
                Address: Get(row, "address") ?? name,
                RoutingType: Get(row, "routingType") ?? "ANYCAST",
                Temporary: Flag(row, "temporary"),
                Internal: Flag(row, "internalQueue"),
                MessageCount: long.TryParse(Get(row, "messageCount"), out var mc) ? mc : 0,
                ConsumerCount: int.TryParse(Get(row, "consumerCount"), out var cc) ? cc : 0));
        }
        return result;
    }

    /// <summary>Queues under their addresses; a queue whose address the list did not carry gets an address of its own.</summary>
    internal static List<ArtemisAddress> Compose(List<ArtemisAddress> addresses, List<ArtemisQueue> queues)
    {
        var byName = addresses.ToDictionary(a => a.Name, StringComparer.Ordinal);
        foreach (var queue in queues)
        {
            if (!byName.TryGetValue(queue.Address, out var address))
            {
                address = new ArtemisAddress(queue.Address, [queue.RoutingType], queue.Internal, queue.Temporary, true, []);
                byName[queue.Address] = address;
                addresses.Add(address);
            }
            address.Queues.Add(queue);
        }
        return addresses;
    }

    // "[\"ANYCAST\",\"MULTICAST\"]" — a JSON array as a string, the way
    // the console gets it; tolerate the bare comma list too.
    private static List<string> ParseRoutingTypes(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return [];
        try
        {
            if (raw.TrimStart().StartsWith('['))
                return JsonSerializer.Deserialize<List<string>>(raw, s_json) ?? [];
        }
        catch (JsonException) { /* fall through to the split */ }
        return raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }

    private static string? Get(Dictionary<string, string> row, string key)
        => row.TryGetValue(key, out var v) ? v : null;

    private static bool Flag(Dictionary<string, string> row, string key)
        => string.Equals(Get(row, key), "true", StringComparison.OrdinalIgnoreCase);
}
