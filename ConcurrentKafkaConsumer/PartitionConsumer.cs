using System.Threading.Channels;

using Confluent.Kafka;

namespace Brnls;

public class PartitionConsumer
{
    public PartitionConsumer(
        CancellationToken gracefulShutdownToken,
        ChannelReader<ConsumeResult<string, byte[]>> messageChanngel,
        Action<ConsumeResult<string, byte[]>> storeOffset)
    {
        GracefulShutdownToken = gracefulShutdownToken;
        MessageChanngel = messageChanngel;
        StoreOffset = storeOffset;
    }

    public CancellationToken GracefulShutdownToken { get; }
    public ChannelReader<ConsumeResult<string, byte[]>> MessageChanngel { get; }
    public Action<ConsumeResult<string, byte[]>> StoreOffset { get; }
}
