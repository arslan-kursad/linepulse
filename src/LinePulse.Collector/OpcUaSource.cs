using LinePulse.Contracts;
using Microsoft.Extensions.Options;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;

namespace LinePulse.Collector;

/// <summary>
/// The machine side of the gateway: an OPC-UA client that mirrors the line
/// PLCs into <see cref="MachineSample"/> events on the spool.
/// </summary>
/// <remarks>
/// <para>
/// In a plant this talks to a connectivity layer that fronts the PLCs over
/// OPC-UA. The collector cannot tell the difference between that and a native
/// OPC-UA server, which is the entire reason to speak OPC-UA at the edge: the
/// gateway survives a change of PLC vendor.
/// </para>
/// <para>
/// Everything here is built for a connection that will be lost. A shop-floor
/// switch reboots during a shift, a firewall rule is changed at lunchtime, a
/// server runs out of session slots; none of those may require a person to
/// restart a service, so the session is rebuilt from scratch with bounded
/// backoff and the node ids are resolved again by browsing, because a server
/// that restarted may well have renumbered them.
/// </para>
/// </remarks>
public sealed class OpcUaSource
{
    private readonly OpcUaOptions _options;
    private readonly EventSpooler _spooler;
    private readonly CollectorMetrics _metrics;
    private readonly ITelemetryContext _telemetry;
    private readonly ILogger<OpcUaSource> _log;

    public OpcUaSource(
        IOptions<CollectorOptions> options,
        EventSpooler spooler,
        CollectorMetrics metrics,
        ITelemetryContext telemetry,
        ILogger<OpcUaSource> log)
    {
        _options = options.Value.OpcUa;
        _spooler = spooler;
        _metrics = metrics;
        _telemetry = telemetry;
        _log = log;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        if (!_options.Enabled)
        {
            _log.LogInformation("OPC-UA source disabled by configuration");
            return;
        }

        // One application instance for the life of the process: it owns the
        // client certificate, which every session after a reconnect reuses.
        await using var application = new ApplicationInstance(_telemetry)
        {
            ApplicationName = _options.ApplicationName,
            ApplicationType = ApplicationType.Client,
        };

        var configuration = await BuildApplicationConfigurationAsync(application, ct);
        var backoff = new Backoff();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunSessionAsync(configuration, backoff, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = backoff.Peek();
                _log.LogWarning(
                    "OPC-UA session to {Endpoint} failed (attempt {Attempt}): {Reason}; retrying in {Delay:0.0}s",
                    _options.EndpointUrl,
                    backoff.Attempt + 1,
                    ex.Message,
                    delay.TotalSeconds);
                _log.LogDebug(ex, "OPC-UA session failure detail");

                try
                {
                    await backoff.DelayAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _log.LogInformation("OPC-UA source stopped");
    }

    /// <summary>
    /// Builds the client's application configuration in code.
    /// </summary>
    /// <remarks>
    /// The stack normally reads an XML file, which means a deployment is a
    /// binary plus a config file that has to match it. For a gateway that gets
    /// copied to another site by hand, one less file to get wrong is worth the
    /// few lines here; the PKI directory is still on disk because a client
    /// certificate has to persist across restarts to be worth anything.
    /// </remarks>
    private async Task<ApplicationConfiguration> BuildApplicationConfigurationAsync(
        ApplicationInstance application,
        CancellationToken ct)
    {
        var pkiRoot = Path.GetFullPath(_options.PkiRoot);
        Directory.CreateDirectory(pkiRoot);
        var subject = $"CN={_options.ApplicationName}, DC={HostName()}";

        var configuration = await application
            .Build(
                applicationUri: $"urn:{HostName()}:LinePulse:Collector",
                productUri: "urn:linepulse:collector")
            .AsClient()
            .SetDefaultSessionTimeout(_options.SessionTimeoutMs)
            .AddSecurityConfiguration(
                ApplicationConfigurationBuilder.CreateDefaultApplicationCertificates(
                    subject,
                    CertificateStoreType.Directory,
                    Path.Combine(pkiRoot, "own")),
                pkiRoot)
            // The lab's simulator presents a self-signed certificate it minted
            // on first start. Accepting it automatically is a lab decision and
            // is called out in the docs; a plant deployment turns this off and
            // puts the server's certificate in the trusted store once, by hand.
            .SetAutoAcceptUntrustedCertificates(true)
            .CreateAsync(ct);

        application.ApplicationConfiguration = configuration;

        // Mints the client certificate on first run; on later runs it just
        // validates the one already in the store.
        if (!await application.CheckApplicationInstanceCertificatesAsync(silent: true, lifeTimeInMonths: null, ct))
        {
            _log.LogWarning("OPC-UA client certificate could not be validated; secured endpoints will refuse this client");
        }

        return configuration;
    }

    private static string HostName() => System.Net.Dns.GetHostName();

    private async Task RunSessionAsync(
        ApplicationConfiguration configuration,
        Backoff backoff,
        CancellationToken ct)
    {
        // Security mode None because that is all the lab server offers; the
        // certificate plumbing above is what makes switching it on later a
        // configuration change.
        var endpointDescription = await CoreClientUtils.SelectEndpointAsync(
            configuration,
            _options.EndpointUrl,
            useSecurity: false,
            _telemetry,
            ct)
            ?? throw new InvalidOperationException(
                $"OPC-UA server at {_options.EndpointUrl} offers no endpoint this client can use");

        var endpoint = new ConfiguredEndpoint(
            null,
            endpointDescription,
            EndpointConfigurationExtensions.Create(configuration));

        // The classic session with the V2 subscription engine: the session
        // lifetime stays in this class's hands (rebuild and re-browse on any
        // failure), while subscriptions use the managed model.
        var factory = new DefaultSessionFactory(_telemetry)
            .WithSubscriptionEngine(DefaultSubscriptionEngineFactory.Instance);

        var session = await factory.CreateAsync(
            configuration,
            endpoint,
            updateBeforeConnect: false,
            checkDomain: false,
            sessionName: _options.ApplicationName,
            sessionTimeout: (uint)_options.SessionTimeoutMs,
            identity: new UserIdentity(),
            preferredLocales: default,
            ct);

        try
        {
            session.KeepAliveInterval = _options.KeepAliveIntervalMs;
            _log.LogInformation(
                "OPC-UA session open to {Endpoint} (security {Mode})",
                endpointDescription.EndpointUrl,
                endpointDescription.SecurityMode);

            var lines = await OpcUaLineBrowser.DiscoverAsync(session, _options.RootObjectName, _log, ct);
            if (lines.Count == 0)
            {
                // Not an error we can fix by reconnecting faster, but not a
                // reason to give up either: the simulator may simply not have
                // finished building its address space yet.
                throw new InvalidOperationException(
                    $"no lines found under Objects/{_options.RootObjectName}");
            }

            // Seeded from the wall clock in milliseconds, not from zero. The
            // spool deduplicates on (Source, Seq) for six hours, so a collector
            // that restarted and began again at 1 would have its first minutes
            // of fresh samples silently rejected as duplicates of the previous
            // run's. A line emits a handful of samples a second, far below the
            // thousand the clock advances, so a new seed always lands above the
            // last sequence number of the run before.
            var seqSeed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var buffers = lines.ToDictionary(
                l => l.Key,
                l => new LineTagBuffer(l.Site, l.Line, seqSeed),
                StringComparer.Ordinal);

            await using var subscription = await MachineSubscription.CreateAsync(
                session, lines, buffers, _options, _log, ct);

            // A session that produced a subscription is a success, whatever
            // happens next.
            backoff.Reset();

            await PumpAsync(session, subscription, buffers.Values.ToList(), ct);
        }
        finally
        {
            try
            {
                await session.CloseAsync(timeout: 2_000, closeChannel: true, CancellationToken.None);
            }
            catch (Exception ex)
            {
                // Closing a session that is already gone is normal during an
                // outage; it must never mask the error that got us here.
                _log.LogDebug(ex, "closing OPC-UA session failed");
            }

            await session.DisposeAsync();
        }
    }

    /// <summary>
    /// Emits accumulated samples and watches the session for signs of death.
    /// </summary>
    private async Task PumpAsync(
        ISession session,
        MachineSubscription subscription,
        IReadOnlyList<LineTagBuffer> buffers,
        CancellationToken ct)
    {
        var emitInterval = TimeSpan.FromMilliseconds(Math.Max(50, _options.SampleEmitIntervalMs));
        var stallTimeout = TimeSpan.FromMilliseconds(Math.Max(1_000, _options.SubscriptionStallTimeoutMs));
        using var timer = new PeriodicTimer(emitInterval);
        var emissions = new List<SampleEmission>();
        var lastReport = DateTimeOffset.UtcNow;

        while (await timer.WaitForNextTickAsync(ct))
        {
            // The stack sets this when keep-alives stop coming back. Throwing
            // hands control to the reconnect loop, which rebuilds the session
            // and re-browses: exactly what a switch reboot needs, with nobody
            // logging in to restart a service.
            if (session.KeepAliveStopped)
            {
                throw new ServiceResultException(
                    StatusCodes.BadNoCommunication,
                    "OPC-UA keep-alive stopped");
            }

            if (subscription.FailureReason(stallTimeout) is { } reason)
            {
                throw new ServiceResultException(StatusCodes.BadNoSubscription, reason);
            }

            foreach (var buffer in buffers)
            {
                emissions.Clear();
                buffer.TakeSamples(emissions);

                foreach (var emission in emissions)
                {
                    if (emission.CounterRestart)
                    {
                        _metrics.OnCounterRestart();
                        _log.LogWarning(
                            "{Key}: counter went backwards (good={Good} scrap={Scrap}); treating as device restart, delta suppressed",
                            buffer.Key,
                            emission.Sample.GoodCount,
                            emission.Sample.ScrapCount);
                    }

                    await _spooler.SpoolAsync(
                        emission.Sample,
                        Subjects.Machine(buffer.Site, buffer.Line),
                        ct);
                }
            }

            if (DateTimeOffset.UtcNow - lastReport >= TimeSpan.FromSeconds(30))
            {
                lastReport = DateTimeOffset.UtcNow;
                ReportLines(buffers, subscription);
            }
        }
    }

    /// <summary>
    /// Logs what each line produced since the last report.
    /// </summary>
    /// <remarks>
    /// These are the deltas derived from the monotonic counters, which is the
    /// only honest way to say "this line made 412 parts in the last 30
    /// seconds". The absolute counter values travel in the samples because the
    /// contract says counters go as the PLC holds them.
    /// </remarks>
    private void ReportLines(IReadOnlyList<LineTagBuffer> buffers, MachineSubscription subscription)
    {
        foreach (var buffer in buffers)
        {
            var report = buffer.TakeReport();
            _log.LogInformation(
                "{Key} state={State} produced=+{Good} scrap=+{Scrap} stateChanges=+{Changes}",
                buffer.Key,
                report.State,
                report.Good,
                report.Scrap,
                report.StateChanges);
        }

        var bad = subscription.TakeBadQualityCount();
        if (bad > 0)
        {
            _log.LogWarning("{Count} tag updates rejected on quality since last report", bad);
        }
    }
}
