using Microsoft.EntityFrameworkCore;

using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

using Worker2;

var builder = Host.CreateApplicationBuilder();
builder.Logging.AddConsole();
ConfigureOpenTelemetry(builder);
builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<WorkerContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=user;Password=password").UseSnakeCaseNamingConvention());
await builder.Build().RunAsync();


static HostApplicationBuilder ConfigureOpenTelemetry(HostApplicationBuilder builder)
{
    builder.Logging.AddOpenTelemetry(logging =>
    {
        logging.IncludeFormattedMessage = true;
        logging.IncludeScopes = true;
    });

    builder.Services.AddOpenTelemetry()
        .ConfigureResource(c => c.AddService($"app_{builder.Configuration["worker_host"]!}", autoGenerateServiceInstanceId: false))
        .WithLogging()
        .WithMetrics(m =>
        {
            m.AddMeter("ConcurrentKafkaConsumer");
        })
        .UseOtlpExporter();

    return builder;
}
