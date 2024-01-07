using Brnls;

using Confluent.Kafka;
using System.Text;

namespace Worker2;

public class Worker : BackgroundService
{
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly WorkerConfig _workerConfig;

    public Worker(ILogger<ConcurrentKafkaConsumer> logger, ILoggerFactory loggerFactory, IServiceScopeFactory serviceScopeFactory, WorkerConfig workerConfig)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _serviceScopeFactory = serviceScopeFactory;
        _workerConfig = workerConfig;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConcurrentKafkaConsumerConfig
        {
            ConsumerConfig = new ConsumerConfig()
            {
                // Because we are buffering messages in memory ourselves we don't want the internal 
                // queues to buffer as much (QueuedMaxMessagesKbytes defaults to 65536 Kb)
                QueuedMaxMessagesKbytes = 1000,
                BootstrapServers = "localhost:9092",
                GroupId = "consumer-1",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoOffsetStore = false,
                EnableAutoCommit = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            },
            GracefulShutdownTimeout = TimeSpan.FromSeconds(2)
        };

        _logger.LogInformation("Starting receiver");

        var topics = new[] { 
            TopicConfiguration.MessageConsumer(
                "topic-name",
                _loggerFactory, 

                async (msg, token) =>
                {
                    using var scope = _serviceScopeFactory.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<WorkerContext>();
                    Console.WriteLine($"Consumed event from topic {msg.TopicPartitionOffset} with key {msg.Message.Key,-10} and value {Encoding.UTF8.GetString(msg.Message.Value)}");
                    context.Results.Add(new Result
                    {
                        Topic = msg.Topic,
                        MessageId = msg.Message.Key,
                        Offset = (int)msg.Offset.Value,
                        Partition = msg.Partition.Value,
                        Host = _workerConfig.Host
                    });
                    await context.SaveChangesAsync(token);
                }),
            TopicConfiguration.BatchMessageConsumer(
                "batch-topic",
                20,
                _loggerFactory,
                (batch, storePartialSuccessOffset, token) =>
                {
                    return Task.CompletedTask;
                })
        };

        using var consumer = new ConcurrentKafkaConsumer(config, topics, _loggerFactory);

        // The consume method should use its own thread (create a new thread or use Task.Factory.StartNew with TaskCreationOptions.LongRunning)
        // to avoid blocking a thread pool thread.
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
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        }, TaskCreationOptions.LongRunning);
    }
}
