# ConcurrentKafkaReceiver

Kafka Receiver for concurrent processing of partitions using Confluent.Kafka

## Getting Started

Sample background worker service

```c#
public class Worker : BackgroundService
{
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly WorkerConfig _workerConfig;

    public Worker(ILogger<ConcurrentKafkaConsumer> logger, ILoggerFactory loggerFactory, IServiceScopeFactory serviceScopeFactory)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _serviceScopeFactory = serviceScopeFactory;
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

        var topics = new[] { "topic-name1", "topic-name2" };
        using var consumer = new ConcurrentKafkaConsumer(config, topics, _loggerFactory);
        _logger.LogInformation("Starting receiver");

        // The consume method should use its own thread (create new thread or use Task.Factory.StartNew with TaskCreationOptions.LongRunning)
        // to avoid blocking a thread pool thread.
        await Task.Factory.StartNew(() =>
        {
            try
            {
                // This call will consume until the stoppingToken is cancelled. Messages in flight will be given time to complete.
                //Offsets are stored each time the message handler is invoked. The cancellation token passed to the handler is the
                // "ungraceful" shutdown token. Once the host stops, the receiver will stop consuming new messages. If the handler
                // doesn't complete GracefulShutdownTimeout time, the token will trigger
                consumer.Consume(async (msg, token) =>
                {
                    using var scope = _serviceScopeFactory.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<WorkerContext>();
                    Console.WriteLine($"Consumed event from topic {msg.TopicPartitionOffset} with key {msg.Message.Key,-10} and value {Encoding.UTF8.GetString(msg.Message.Value)}");
                    //await Task.Delay(10, token);
                }, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        }, TaskCreationOptions.LongRunning);
    }
}
```
