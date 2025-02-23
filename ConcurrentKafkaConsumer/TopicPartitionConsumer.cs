using System.Threading.Channels;

using Confluent.Kafka;

namespace Brnls;

public class TopicPartitionConsumer
{
    public TopicPartitionConsumer(
        CancellationToken gracefulShutdownToken,
        ChannelReader<ConsumeResult<string, byte[]>> messageChanngel,
        Action<ConsumeResult<string, byte[]>> storeOffset,
        TopicPartition topicPartition)
    {
        GracefulShutdownToken = gracefulShutdownToken;
        MessageChanngel = messageChanngel;
        StoreOffset = storeOffset;
        TopicPartition = topicPartition;
    }

    /// <summary>
    /// Cancelled when either the topic partition is revoked from this consumer or when the 
    /// cancellation token passed to <see cref="KafkaConsumer.Consume(CancellationToken)"/>
    /// is cancelled. When a partition is revoked there is a grace period that allows 
    /// wrapping up processing and storing offset for the currently inflight messages.
    /// After that timeout the the consumer group will rebalance. See librdkafka 'max.poll.interval.ms'
    /// </summary>
    public CancellationToken GracefulShutdownToken { get; }
    public ChannelReader<ConsumeResult<string, byte[]>> MessageChanngel { get; }
    public Action<ConsumeResult<string, byte[]>> StoreOffset { get; }
    public TopicPartition TopicPartition { get; }
}
