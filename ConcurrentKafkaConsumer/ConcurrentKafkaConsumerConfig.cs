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
    /// The time to allow to finish current processing of messages in flight after the
    /// cancellation token passed to "Receive" is cancelled. Once this time elapses,
    /// the cancellation token passed to the receive message handler is cancelled.
    /// Defaults to 5 seconds.
    /// </summary>
    public TimeSpan GracefulShutdownTimeout { get; init;  } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The maximum number of topic partitions that will be processed concurrently.
    /// Defaults to the environment processor count.
    /// </summary>
    public int MaxConcurrency { get; init; } = Environment.ProcessorCount;
}
