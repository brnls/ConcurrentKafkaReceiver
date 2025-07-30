using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace TestHost;
public class AspireDashboardService : IContainerInit
{
    private readonly IContainer _container;

    public AspireDashboardService()
    {
        _container = new ContainerBuilder()
            .WithName("aspire-dashboard")
            .WithImage("mcr.microsoft.com/dotnet/aspire-dashboard:9.1")
            .WithPortBinding(18888, 18888)
            .WithPortBinding(4317, 18889)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS"] = "true",
            })
            .Build();
    }

    public ValueTask DisposeAsync()
    {
        return _container.DisposeAsync();
    }

    public Task InitAsync(CancellationToken token)
    {
        return _container.StartAsync(token);
    }
}
