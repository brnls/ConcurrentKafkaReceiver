sealed class ContainerManager : IAsyncDisposable
{
    private readonly IEnumerable<IContainerInit> _services;

    public ContainerManager(IEnumerable<IContainerInit> services)
    {
        _services = services;
    }

    public async Task InitAllAsync(CancellationToken token)
    {
        foreach (var service in _services)
        {
            await service.InitAsync(token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var service in _services)
        {
            await service.DisposeAsync();
        }
    }
}
