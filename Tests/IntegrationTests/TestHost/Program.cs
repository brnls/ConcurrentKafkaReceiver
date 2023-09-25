using Microsoft.EntityFrameworkCore;
using TestHost;

var builder = Host.CreateApplicationBuilder();
builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<TestHostContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=postgres;Password=postgres").UseSnakeCaseNamingConvention());
await builder.Build().RunAsync();
