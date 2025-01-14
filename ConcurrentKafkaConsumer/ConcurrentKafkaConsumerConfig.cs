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
}
