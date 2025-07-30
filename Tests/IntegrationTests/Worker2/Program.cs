using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Net.Http.Headers;
using OpenTelemetry;
using OpenTelemetry.Resources;

using Worker2;


var builder = WebApplication.CreateBuilder(args);
builder.Logging.AddConsole();
ConfigureOpenTelemetry(builder);
builder.Services.AddSingleton<Worker>();
//builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<WorkerContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=user;Password=password").UseSnakeCaseNamingConvention());
var app = builder.Build();


app.MapPost("/sse", async (
    HttpContext ctx,
    Worker worker,
    [FromBody] Dictionary<string, string> config,
    CancellationToken token) =>
{
    ctx.Response.Headers.Append(HeaderNames.ContentType, "text/event-stream");
    int counter = 0;
    var tpBuffer = int.Parse(config["tp.buffer"]);
    config.Remove("tp.buffer");
    _ = worker.Run(token, config, tpBuffer);

    while (!token.IsCancellationRequested)
    {
        await ctx.Response.WriteAsync($"data: {Telemetry.GetItemsPerSecond()}\n\n");
        await Task.Delay(3000, token);
        counter++;
    }
});

await app.RunAsync();



static WebApplicationBuilder ConfigureOpenTelemetry(WebApplicationBuilder builder)
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
            //m.AddMeter("Worker");
        })
        .UseOtlpExporter();

    return builder;
}
