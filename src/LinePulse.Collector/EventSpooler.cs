using System.Text.Json;
using LinePulse.Contracts;

namespace LinePulse.Collector;

/// <summary>
/// The only way an event may enter the pipeline: serialise it, write it to the
/// spool, count it.
/// </summary>
/// <remarks>
/// Both sources go through here and neither one can see the NATS connection.
/// That is the design, not an accident of layering: the moment a source is
/// allowed to publish straight to the network, the guarantee changes from "we
/// have it on disk" to "we had it in memory", and the first switch reboot
/// costs an hour of traceability.
/// </remarks>
public sealed class EventSpooler
{
    private readonly Spool _spool;
    private readonly CollectorMetrics _metrics;
    private readonly ILogger<EventSpooler> _log;

    public EventSpooler(Spool spool, CollectorMetrics metrics, ILogger<EventSpooler> log)
    {
        _spool = spool;
        _metrics = metrics;
        _log = log;
    }

    /// <summary>
    /// Writes one event to the spool under the subject it will be published on.
    /// </summary>
    /// <returns><c>true</c> if stored, <c>false</c> if rejected as a duplicate.</returns>
    public async Task<bool> SpoolAsync<T>(T evt, string subject, CancellationToken ct)
        where T : EventEnvelope
    {
        _metrics.OnReceived(evt);

        // Serialised against the concrete type so every field of a
        // MachineSample or TestRecord is written, not just the envelope.
        var payload = JsonSerializer.SerializeToUtf8Bytes(evt, JsonDefaults.Options);

        var stored = await _spool.EnqueueAsync(
            subject,
            payload,
            evt.Source,
            evt.Seq,
            evt.EventTimeUtc,
            ct);

        if (stored)
        {
            _metrics.OnSpooled();
            return true;
        }

        _metrics.OnDuplicateRejected();
        // Debug, not warning: duplicates are expected traffic under
        // at-least-once delivery, and a line of log per duplicate during a
        // bench retry storm would bury the entries that matter.
        _log.LogDebug("duplicate rejected {Key} on {Subject}", evt.Key, subject);
        return false;
    }
}
