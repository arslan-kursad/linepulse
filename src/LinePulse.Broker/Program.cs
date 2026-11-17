using LinePulse.Broker;

// LinePulse Broker -- the lab's stand-in for the plant MQTT broker.

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    // Settings come from next to the binary rather than from the working
    // directory, so "dotnet run --project" from the repository root and a
    // service manager both read the same appsettings.json.
    ContentRootPath = AppContext.BaseDirectory,
});

builder.Services.Configure<BrokerOptions>(builder.Configuration.GetSection(BrokerOptions.Section));
builder.Services.AddHostedService<MqttBrokerService>();

await builder.Build().RunAsync();
