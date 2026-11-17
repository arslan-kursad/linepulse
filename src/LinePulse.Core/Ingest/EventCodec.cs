using System.Text.Json;
using LinePulse.Contracts;

namespace LinePulse.Core.Ingest;

/// <summary>Why an event could not be accepted.</summary>
public enum DecodeFailure
{
    None,
    UnknownSubject,
    Malformed,
    MissingKey,
}

/// <summary>
/// Turns a NATS message into a typed event.
/// </summary>
/// <remarks>
/// <para>
/// The subject carries the type, so the payload does not need a discriminator
/// field that a firmware update could rename. Unknown members are ignored by
/// <c>JsonDefaults</c> on purpose: a bench vendor adding a field in a service pack
/// must not stop a plant's traceability.
/// </para>
/// <para>
/// An event with no <c>source</c> or no positive <c>seq</c> is refused rather than
/// stored. Without an idempotency key there is no way to tell its republish apart
/// from a new part. A missing <c>seq</c> is the dangerous one: it deserialises as
/// 0, so every event from that producer would share one key and all but the first
/// would be filed as duplicates -- new parts lost under a counter that looks like
/// normal at-least-once traffic. Producer counters start at 1.
/// </para>
/// </remarks>
public static class EventCodec
{
    public const string MachineSuffix = ".machine";
    public const string TestSuffix = ".test";

    public static (EventEnvelope? Event, DecodeFailure Failure) Decode(string subject, ReadOnlySpan<byte> payload)
    {
        EventEnvelope? decoded;
        try
        {
            if (subject.EndsWith(MachineSuffix, StringComparison.Ordinal))
            {
                decoded = JsonSerializer.Deserialize<MachineSample>(payload, JsonDefaults.Options);
            }
            else if (subject.EndsWith(TestSuffix, StringComparison.Ordinal))
            {
                decoded = JsonSerializer.Deserialize<TestRecord>(payload, JsonDefaults.Options);
            }
            else
            {
                return (null, DecodeFailure.UnknownSubject);
            }
        }
        catch (JsonException)
        {
            return (null, DecodeFailure.Malformed);
        }

        if (decoded is null)
        {
            return (null, DecodeFailure.Malformed);
        }

        if (string.IsNullOrWhiteSpace(decoded.Source)
            || decoded.Seq <= 0
            || string.IsNullOrWhiteSpace(decoded.Site)
            || string.IsNullOrWhiteSpace(decoded.Line)
            || decoded.EventTimeUtc == default)
        {
            return (null, DecodeFailure.MissingKey);
        }

        return (decoded, DecodeFailure.None);
    }
}
