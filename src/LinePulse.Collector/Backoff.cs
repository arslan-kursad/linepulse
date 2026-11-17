namespace LinePulse.Collector;

/// <summary>
/// Bounded exponential backoff with jitter, used by every reconnect loop here.
/// </summary>
/// <remarks>
/// <para>
/// Unbounded backoff and no backoff are both wrong on a shop floor. Retrying
/// flat out turns one dead switch into a log file nobody can read and a PLC
/// answering connection attempts instead of running the line; backing off to
/// minutes means the line is back but the data is not.
/// </para>
/// <para>
/// The jitter matters more than it looks. Thirteen collectors that all lost the
/// same switch would otherwise reconnect in lockstep and hit the server
/// together, every time, forever.
/// </para>
/// </remarks>
public sealed class Backoff
{
    private readonly TimeSpan _min;
    private readonly TimeSpan _max;
    private int _attempt;

    public Backoff(TimeSpan? min = null, TimeSpan? max = null)
    {
        _min = min ?? BackoffDefaults.Min;
        _max = max ?? BackoffDefaults.Max;
    }

    /// <summary>Number of failures since the last <see cref="Reset"/>.</summary>
    public int Attempt => _attempt;

    /// <summary>Called after anything that counts as success, so the next outage starts short again.</summary>
    public void Reset() => _attempt = 0;

    /// <summary>The delay the next attempt would wait, without waiting it.</summary>
    public TimeSpan Peek()
    {
        // 1s, 2s, 4s ... capped. Shifted rather than Math.Pow to keep it
        // integral, and clamped at 20 shifts so the doubling cannot overflow
        // during a long outage.
        var shift = Math.Min(_attempt, 20);
        var scaled = _min.Ticks * (1L << shift);
        var capped = Math.Min(scaled, _max.Ticks);
        var jitter = Random.Shared.NextDouble() * 0.2 - 0.1; // +/-10%
        return TimeSpan.FromTicks((long)(capped * (1.0 + jitter)));
    }

    /// <summary>Waits out the current delay and counts the attempt.</summary>
    public async Task<TimeSpan> DelayAsync(CancellationToken ct)
    {
        var delay = Peek();
        _attempt++;
        await Task.Delay(delay, ct);
        return delay;
    }
}
