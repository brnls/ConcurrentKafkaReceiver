using Microsoft.EntityFrameworkCore;

using Worker2;

var conf = new WorkerConfig(args[0]);
var builder = Host.CreateApplicationBuilder();
builder.Services.AddSingleton(conf);
builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<WorkerContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=postgres;Password=postgres").UseSnakeCaseNamingConvention());
await builder.Build().RunAsync();


public record WorkerConfig(string Host);
