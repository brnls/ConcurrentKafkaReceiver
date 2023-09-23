# ConcurrentKafkaReceiver
Kafka Receiver for concurrent processing of partitions using Confluent.Kafka


## Getting Started

Sample background worker service
```c#
public class Worker : BackgroundService
{
    private readonly ILogger<ConcurrentKafkaReceiver> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IServiceScopeFactory _serviceScopeFactory;

    public Worker(ILogger<ConcurrentKafkaReceiver> logger, ILoggerFactory loggerFactory, IServiceScopeFactory serviceScopeFactory, WorkerConfig workerConfig)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig()
        {
            QueuedMaxMessagesKbytes = 1000,
            BootstrapServers = "localhost:9092",
            GroupId = "get-star5",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoOffsetStore = false,
            EnableAutoCommit = true,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };

        const string topic = "topic-name";
        using var receiver = new ConcurrentKafkaReceiver(consumerConfig, new[] { topic }, _loggerFactory);
        _logger.LogInformation("Starting receiver");

	// this will receive until the stopping token is cancelled. Messages in flight will be given time to complete.
	//Offsets are stored each time the message handler is invoked. The cancellation token passed to the handler is the "ungraceful" shutdown token. Once the host stops, the receiver will stop consuming new messages. If the handler doesn't complete in a small amount of time, the token will trigger
        await receiver.Receive(async (msg, token) =>
        {
            Console.WriteLine($"Consumed event from topic {topic} with key {msg.Message.Key,-10} and value {Encoding.UTF8.GetString(msg.Message.Value)}");
            await Task.Delay(1000, token);
        }, stoppingToken);
    }
}
```
