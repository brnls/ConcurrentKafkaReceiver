using System.Diagnostics.Metrics;

namespace Brnls;
public class ConsumerMetrics
{
    private static readonly Meter _meter = new Meter("ConcurrentKafkaConsumer");

    public static readonly Gauge<int> BatchConsumerBatchSize = _meter.CreateGauge<int>("batch.size");
}
