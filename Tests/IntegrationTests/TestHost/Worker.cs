using System.Threading.Channels;

using CliWrap;

namespace TestHost;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;

    public Worker(ILogger<Worker> logger, IServiceScopeFactory serviceScopeFactory)
    {
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
        var processTasks = Channel.CreateUnbounded<Task>();

        try
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                processTasks.Writer.TryWrite(Task.Run(async () =>
                {
                    var host = Guid.NewGuid().ToString();
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                    using var l = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, cts.Token);
                    await RunConsumer(host, l.Token);
                }));
            }
        }
        catch (OperationCanceledException e) when (e.CancellationToken == stoppingToken) { }

        processTasks.Writer.Complete();
        await foreach(var p in processTasks.Reader.ReadAllAsync())
        {
            try
            {
                _logger.LogInformation("waiting for process task");
                await p;
                _logger.LogInformation("finished waiting for process task");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "finished waiting for process task");
            }
        }

        async Task RunConsumer(string host, CancellationToken stoppingToken)
        {
            _logger.LogInformation("Starting host {host}", host);
            using var scope = _serviceScopeFactory.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<TestHostContext>();
            var logChannel = Channel.CreateUnbounded<string>();
            var processTask = Cli.Wrap("C:/Code/ConcurrentKafkaReceiver/Tests/IntegrationTests/Worker2/bin/release/net7.0/Worker2.exe")
                .WithArguments(host)
                .WithStandardOutputPipe(PipeTarget.ToDelegate(s => logChannel.Writer.TryWrite(s)))
                .ExecuteAsync(default, stoppingToken);


            stoppingToken.Register(() => logChannel.Writer.Complete());
            var count = 0;
            while (await logChannel.Reader.WaitToReadAsync())
            {
                while (logChannel.Reader.TryRead(out var log))
                {
                    context.Logs.Add(new Log { Host = host, Message = log });
                    count++;
                    if (count % 10 == 0)
                    {
                        await context.SaveChangesAsync();
                    }
                }
            }
            await context.SaveChangesAsync();
            await processTask;
        };
    }
}
