using LinePulse.Contracts;

namespace LinePulse.Collector;

/// <summary>
/// The handful of numbers that tell you whether the edge is healthy.
/// </summary>
/// <remarks>
/// Deliberately tiny and deliberately not a metrics library. During a shift
/// handover nobody opens a dashboard for the gateway itself: they read the last
/// line of its log. <c>received</c> rising with <c>forwarded</c> flat and the
/// spool depth climbing is the whole diagnosis of a broken uplink, in one line.
/// </remarks>
public sealed class CollectorMetrics
{
    private long _receivedMachineSamples;
    private long _receivedTestRecords;
    private long _spooled;
    private long _duplicatesRejected;
    private long _forwarded;
    private long _batchesResent;
    private long _deserializationFailures;
    private long _counterRestarts;

    /// <summary>Events taken off either protocol, before deduplication.</summary>
    public long Received => ReceivedMachineSamples + ReceivedTestRecords;

    /// <summary>Machine samples built from OPC-UA tag changes.</summary>
    public long ReceivedMachineSamples => Interlocked.Read(ref _receivedMachineSamples);

    /// <summary>Test records taken off MQTT, duplicates included.</summary>
    public long ReceivedTestRecords => Interlocked.Read(ref _receivedTestRecords);

    /// <summary>Events written to disk, and therefore owed to NATS.</summary>
    public long Spooled => Interlocked.Read(ref _spooled);

    /// <summary>
    /// Events dropped because <c>(Source, Seq)</c> was already handled. On a
    /// healthy lab this tracks the simulator's <c>LP_DUP_RATE</c>, which is how
    /// we know the check is doing something.
    /// </summary>
    public long DuplicatesRejected => Interlocked.Read(ref _duplicatesRejected);

    /// <summary>Events NATS has confirmed, and therefore gone from the spool.</summary>
    public long Forwarded => Interlocked.Read(ref _forwarded);

    /// <summary>
    /// Batches published a second time because the connection changed before
    /// the confirmation came back. Each one puts duplicates on the subject,
    /// which ingest absorbs; this says how many to expect.
    /// </summary>
    public long BatchesResent => Interlocked.Read(ref _batchesResent);

    /// <summary>Payloads that were not valid events. Real benches do emit rubbish.</summary>
    public long DeserializationFailures => Interlocked.Read(ref _deserializationFailures);

    /// <summary>Times a monotonic counter jumped backwards, i.e. a device restarted.</summary>
    public long CounterRestarts => Interlocked.Read(ref _counterRestarts);

    public void OnReceived(EventEnvelope evt)
    {
        if (evt is MachineSample)
        {
            Interlocked.Increment(ref _receivedMachineSamples);
        }
        else
        {
            Interlocked.Increment(ref _receivedTestRecords);
        }
    }

    public void OnSpooled() => Interlocked.Increment(ref _spooled);

    public void OnDuplicateRejected() => Interlocked.Increment(ref _duplicatesRejected);

    public void OnForwarded(long count) => Interlocked.Add(ref _forwarded, count);

    public void OnBatchResent() => Interlocked.Increment(ref _batchesResent);

    public void OnDeserializationFailure() => Interlocked.Increment(ref _deserializationFailures);

    public void OnCounterRestart() => Interlocked.Increment(ref _counterRestarts);
}
