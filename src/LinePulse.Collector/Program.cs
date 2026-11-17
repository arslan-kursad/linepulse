using LinePulse.Collector;
using Microsoft.Extensions.Options;
using Opc.Ua;

// LinePulse Collector -- the edge gateway between the lines and the plant network.
//
// One process per site box: it reads the line PLCs over OPC-UA and the
// end-of-line benches over MQTT, writes every event to a local spool, and drains
// the spool to NATS. Nothing reaches NATS except through the spool, which is what
// lets the uplink go down for an hour without the plant losing an hour of data.

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    // Configuration comes from next to the binary, not from the working
    // directory, so the gateway reads the same settings whether it is started by
    // hand, by a script or by a service manager that starts it from / or from
    // C:\Windows\System32. Data paths in those settings stay relative to the
    // working directory, so a dev run keeps its spool where sqlite3 can find it.
    ContentRootPath = AppContext.BaseDirectory,
});

builder.Services.Configure<CollectorOptions>(builder.Configuration.GetSection(CollectorOptions.Section));

builder.Services.AddSingleton<CollectorMetrics>();
builder.Services.AddSingleton(sp => new Spool(
    sp.GetRequiredService<IOptions<CollectorOptions>>().Value.Spool.Path,
    sp.GetRequiredService<ILogger<Spool>>()));
builder.Services.AddSingleton<EventSpooler>();

// The OPC-UA stack logs through the host's logger factory, so its warnings land
// in the same log, under the same filters, as everything else.
builder.Services.AddSingleton<ITelemetryContext>(sp => new ServiceProviderTelemetryContext(sp));

builder.Services.AddSingleton<OpcUaSource>();
builder.Services.AddSingleton<MqttSource>();
builder.Services.AddSingleton<NatsForwarder>();
builder.Services.AddSingleton<SpoolMonitor>();
builder.Services.AddHostedService<CollectorWorker>();

// Ctrl+C and SIGTERM cancel the components' token; each one closes its own
// connection and the spool is disposed only after all of them have returned.
await builder.Build().RunAsync();
