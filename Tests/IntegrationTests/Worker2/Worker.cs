using Brnls;

using Confluent.Kafka;
using System.Text;
using System.Text.Json;

namespace Worker2;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IConfiguration _config;

    public Worker(ILogger<Worker> logger, ILoggerFactory loggerFactory, IServiceScopeFactory serviceScopeFactory, IConfiguration config)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _serviceScopeFactory = serviceScopeFactory;
        _config = config;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.CompletedTask;
    }

    public async Task Run(
        CancellationToken stoppingToken,
        Dictionary<string, string> rawConfig,
        int tpBuffer)
    {
        var host = _config["worker_host"] ?? "undefined";
        var config = new ConsumerConfig(rawConfig)
        {
            // Because we are buffering messages in memory ourselves we don't want the internal 
            // queues to buffer as much (QueuedMaxMessagesKbytes defaults to 65536 Kb)
            //QueuedMaxMessagesKbytes = 10000,
            BootstrapServers = "localhost:9092",
            GroupId = "consumer-1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false,
            EnableAutoCommit = true,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            StatisticsIntervalMs = 5000,
            //Debug = "consumer,topic"
        };

        _logger.LogInformation("Starting receiver");

        var consumer = new KafkaConsumer(
            config,
            ["topic-name" ],
            _loggerFactory,
            HandleTopicPartition,
            tpBuffer,
            s =>
            {
                using var scope = _serviceScopeFactory.CreateScope();
                var json = JsonDocument.Parse(s);
                var workerContext = scope.ServiceProvider.GetRequiredService<WorkerContext>();
                workerContext.Stats.Add(new Stats
                {
                    Host = host,
                    Value = JsonSerializer.Serialize(json, new JsonSerializerOptions { WriteIndented = true }),
                    CreatedAt = DateTime.UtcNow,
                });
                workerContext.SaveChanges();

            });

        var sconsumer = new SimplerConsumer(config, ["topic-name", "batch-topic"], _loggerFactory, async (cr, so, ct) =>
        {
            await HandleMessage(cr, so, ct);
        });

        // The consume method should use its own thread (create a new thread or use Task.Factory.StartNew with TaskCreationOptions.LongRunning)
        // to avoid blocking a thread pool thread., "batch-topic", "topic1", "topic2", "topic3", "topic4" 
        await Task.Factory.StartNew(() =>
        {
            try
            {
                // This call will consume until the stoppingToken is cancelled. Messages in flight will be given time to complete.
                // but new messages will not be passed to the message handler. If the handler
                // doesn't complete in GracefulShutdownTimeout time, the token will trigger
                //
                // Offsets are stored each time the message handler is invoked. The cancellation token passed to the handler is the
                // forceful shutdown token. Once the host stops, the receiver will stop consuming new messages. If the handler
                // doesn't complete GracefulShutdownTimeout time, the token will trigger
                consumer.Consume(stoppingToken);
                //sconsumer.Consume(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        }, TaskCreationOptions.LongRunning);

        Task HandleTopicPartition(TopicPartitionConsumer tpc)
        {
            return tpc.TopicPartition.Topic switch
            {
                "topic-name" => new MessageConsumer(tpc, HandleMessage, _loggerFactory.CreateLogger<MessageConsumer>()).ProcessPartition(),
                "batch-topic" => new BatchMessageConsumer(tpc, async (batch, storePartialSuccessOffset, token) =>
                {
                    if (batch.Count == 0) throw new Exception("expected item in batch");
                    using var cts = new CancellationTokenSource();
                    await using var _ = token.Register(() => cts.CancelAfter(TimeSpan.FromSeconds(5)));
                    using var scope = _serviceScopeFactory.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<WorkerContext>();
                    foreach (var msg in batch)
                    {
                        context.Results.Add(new Result
                        {
                            Topic = msg.Topic,
                            MessageId = msg.Message.Key,
                            Offset = (int)msg.Offset.Value,
                            Partition = msg.Partition.Value,
                            Host = _config["worker_host"]!
                        });
                    }
                    _logger.LogInformation("Consumed batch {TopicPartition}", batch[^1].TopicPartitionOffset);
                    await context.SaveChangesAsync(cts.Token);
                    await Task.Delay(100, cts.Token);
                },
                20,
                _loggerFactory.CreateLogger<BatchMessageConsumer>()).ProcessPartition(),
                _ => throw new Exception("Unknown topic")
            };
        }
    }

    private async Task HandleMessage(
        ConsumeResult<string, byte[]> msg,
        Action<ConsumeResult<string, byte[]>> storeOffset,
        CancellationToken token)
    {
        using var scope = _serviceScopeFactory.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<WorkerContext>();
        //_logger.LogInformation("Consumed event from topic {TopicPartition} with value {value}", msg.TopicPartitionOffset, Encoding.UTF8.GetString(msg.Message.Value));
        //context.Results.Add(new Result
        //{
        //    Topic = msg.Topic,
        //    MessageId = msg.Message.Key,
        //    Offset = (int)msg.Offset.Value,
        //    Partition = msg.Partition.Value,
        //    Host = _config["worker_host"]!
        //});
        //await context.SaveChangesAsync(token);
        await Task.Delay(7, token);
        storeOffset(msg);
        Telemetry.RecordProcessedItems(1);
    }
}
