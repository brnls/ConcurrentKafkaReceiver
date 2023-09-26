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
                QueuedMaxMessagesKbytes = 1000,
                BootstrapServers = "localhost:9092",
                GroupId = "ppause-consumer-8",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
                EnableAutoCommit = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            },
            GracefulShutdownTimeout = TimeSpan.FromSeconds(5)
        };

        var topics = new[] { "final-topic", "pause-topic" };
        //var topics = new[] { "pause-topic" };
        using var consumer = new ConcurrentKafkaConsumer(config, topics, _loggerFactory);
        _logger.LogInformation("Starting receiver");
        try
        {
            await consumer.Consume(async (msg, token) =>
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
                //await Task.Delay(10, token);
            }, stoppingToken);
        }
        catch(OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }
}
