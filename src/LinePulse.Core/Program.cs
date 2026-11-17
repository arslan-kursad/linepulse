using System.Text.Json.Serialization;
using LinePulse.Core;
using LinePulse.Core.Api;
using LinePulse.Core.Ingest;
using LinePulse.Core.Kpi;
using LinePulse.Core.Realtime;
using LinePulse.Core.Storage;
using Microsoft.Extensions.Options;

// LinePulse Core -- ingest, KPI and API in one process.
//
// One process rather than three because this is a plant-floor deployment: there is
// no platform team, there is an industrial PC in a cabinet, and every extra service
// is another thing that can be left stopped after a power cut. Ingest, aggregation
// and the API are separate classes with separate concerns, and they would split
// cleanly if a site ever outgrew one box -- but they ship as one unit on purpose.

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<NatsOptions>(builder.Configuration.GetSection(NatsOptions.Section));
builder.Services.Configure<StorageOptions>(builder.Configuration.GetSection(StorageOptions.Section));
builder.Services.Configure<IngestOptions>(builder.Configuration.GetSection(IngestOptions.Section));
builder.Services.Configure<SpcOptions>(builder.Configuration.GetSection(SpcOptions.Section));
builder.Services.Configure<ShiftOptions>(builder.Configuration.GetSection(ShiftOptions.Section));
builder.Services.Configure<LiveOptions>(builder.Configuration.GetSection(LiveOptions.Section));

// The plain option objects as well as IOptions<T>: the ingest classes take their
// settings by value because they are not reconfigurable at runtime, and pretending
// otherwise would invite someone to change the watermark mid-shift.
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<IngestOptions>>().Value);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<SpcOptions>>().Value);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<LiveOptions>>().Value);

builder.Services.AddSingleton(TimeProvider.System);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    // Enums as names, not ordinals: "Faulted" survives someone inserting a new
    // state into the middle of the enum, and 3 does not.
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

builder.Services.AddCors(options =>
{
    // Only the Angular dev server. In production the dashboard is served from
    // wwwroot by this same process, so it is same-origin and needs no policy.
    options.AddPolicy("dashboard-dev", policy => policy
        .WithOrigins("http://localhost:4200")
        .AllowAnyHeader()
        .AllowAnyMethod());
});

StorageRegistration.AddStorage(builder.Services, builder.Configuration);

builder.Services.AddSingleton<IngestCounters>();
builder.Services.AddSingleton<MinuteBucketQueue>();
builder.Services.AddSingleton<WatermarkTracker>();
builder.Services.AddSingleton<DedupGate>();
builder.Services.AddSingleton<DowntimeTracker>();
builder.Services.AddSingleton<LineSnapshotCache>();
builder.Services.AddSingleton<LiveFeed>();
builder.Services.AddSingleton<IngestPipeline>();
builder.Services.AddSingleton<ShiftCalendar>();
builder.Services.AddSingleton<SpcAnalyzer>();
builder.Services.AddSingleton<KpiService>();

builder.Services.AddSingleton<NatsIngestService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<NatsIngestService>());
builder.Services.AddHostedService<BucketFlushService>();
builder.Services.AddHostedService<KpiTickService>();

var app = builder.Build();

// Schema before anything can read or write it, and before the subscriber starts.
await app.Services.GetRequiredService<IEventStore>().InitializeAsync(CancellationToken.None);

app.UseCors("dashboard-dev");
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapLinePulseApi();

// The Angular dashboard is a single-page app: deep links have to reach index.html
// instead of 404ing. Only wired up when a build is actually present, so running
// the API alone does not fail at startup.
var indexPath = Path.Combine(app.Environment.WebRootPath ?? "wwwroot", "index.html");
if (File.Exists(indexPath))
{
    app.MapFallbackToFile("index.html");
}

app.Run();
