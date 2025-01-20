using Microsoft.EntityFrameworkCore;

using Worker2;

var builder = Host.CreateApplicationBuilder();
builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<WorkerContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=user;Password=password").UseSnakeCaseNamingConvention());
await builder.Build().RunAsync();
