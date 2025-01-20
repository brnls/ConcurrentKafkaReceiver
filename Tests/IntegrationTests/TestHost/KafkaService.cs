using System.Text;

using CliWrap;

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

sealed class KafkaService : IContainerInit
{
    private readonly IContainer _kafka;

    public KafkaService()
    {
        _kafka = new ContainerBuilder()
            .WithImage("confluentinc/cp-kafka:7.5.0")
            .WithPortBinding(9092, 9092)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["KAFKA_BROKER_ID"] = "1",
                ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                ["KAFKA_ADVERTISED_LISTENERS"] = "PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:9092",
                ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
                ["KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"] = "0",
                ["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"] = "1",
                ["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] = "1",
                ["KAFKA_PROCESS_ROLES"] = "broker,controller",
                ["KAFKA_NODE_ID"] = "1",
                ["KAFKA_CONTROLLER_QUORUM_VOTERS"] = "1@localhost:29093",
                ["KAFKA_LISTENERS"] = "PLAINTEXT://localhost:29092,CONTROLLER://localhost:29093,PLAINTEXT_HOST://0.0.0.0:9092",
                ["KAFKA_INTER_BROKER_LISTENER_NAME"] = "PLAINTEXT",
                ["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER",
                ["KAFKA_LOG_DIRS"] = "/tmp/kraft-combined-logs",
                ["CLUSTER_ID"] = "MkU3OEVBNTcwNTJENDM2Qk",
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9092))
            .Build();
    }

    public async Task InitAsync(CancellationToken token)
    {
        await _kafka.StartAsync(token);
        foreach (var topic in new List<string> { "topic-name", "batch-topic" })
        {
            var sb = new StringBuilder();
            var result = await Cli.Wrap("docker")
                .WithArguments($"exec {_kafka.Name} kafka-topics --create --topic {topic} --partitions 7 --replication-factor 1 --bootstrap-server localhost:9092")
                .WithStandardOutputPipe(PipeTarget.ToStream(Stream.Null))
                .ExecuteAsync(token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _kafka.DisposeAsync();
    }
}
