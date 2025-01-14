using TestHost;
using Microsoft.EntityFrameworkCore;


var builder = Host.CreateApplicationBuilder();
builder.Services.AddSingleton<ContainersService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ContainersService>());
builder.Services.AddHostedService<Worker>();
builder.Services.AddDbContext<TestHostContext>(o =>
    o.UseNpgsql("Host=localhost:5432;Database=postgres;Username=user;Password=password").UseSnakeCaseNamingConvention());
await builder.Build().RunAsync();
