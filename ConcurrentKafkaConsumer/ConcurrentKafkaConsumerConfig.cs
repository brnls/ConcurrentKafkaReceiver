using Confluent.Kafka;

namespace Brnls;

public class ConcurrentKafkaConsumerConfig
{
    /// <summary>
    /// Confluent ConsumerConfig. 
    ///     EnableAutoOffsetStore must be false.
    ///     EnableAutoCommit must be true.
    ///     PartitionAssignmentStrategy must be CooperativeSticky,
    /// </summary>
    public required ConsumerConfig ConsumerConfig { get; init; }

    /// <summary>
    /// The time to allow in-flight messages to finish processing after the 
    /// cancellation token passed to <see cref="ConcurrentKafkaConsumer.Consume(MessageHandler, CancellationToken)"/> 
    /// is cancelled. After that time, the cancellation token passed to <see cref="MessageHandler"/> will be cancelled.
    /// </summary>
    public TimeSpan GracefulShutdownTimeout { get; init;  } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The maximum number of topic partitions that will be processed concurrently.
    /// Defaults to the environment processor count.
    /// </summary>
    public int MaxConcurrency { get; init; } = Environment.ProcessorCount;
}
