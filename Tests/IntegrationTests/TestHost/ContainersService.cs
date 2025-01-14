class ContainersService() : BackgroundService
{
    private readonly TaskCompletionSource _containersInitializedTc = new();
    public Task ContainersInitialized => _containersInitializedTc.Task;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var _ = await InitContainers(stoppingToken);
        _containersInitializedTc.SetResult();

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken).ContinueWith(_ => { }, CancellationToken.None);
        }
    }

    private async Task<IAsyncDisposable> InitContainers(CancellationToken stoppingToken)
    {
        var containerManager = new ContainerManager(
        [
            new KafkaService(),
            new PostgresService()
        ]);

        await containerManager.InitAllAsync(stoppingToken);
        return containerManager;
    }
}

interface IContainerInit : IAsyncDisposable
{
    Task InitAsync(CancellationToken token);
}