using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Brnls;

public sealed class SimplerConsumer
{
    private readonly ConsumerConfig _config;
    private readonly IEnumerable<string> _topics;
    private readonly MessageHandler _handleTopicPartition;
    private readonly ILogger<KafkaConsumer> _logger;

    public SimplerConsumer(
        ConsumerConfig config,
        IEnumerable<string> topics,
        ILoggerFactory loggerFactory,
        MessageHandler handleTopicPartition)
    {
        _logger = loggerFactory.CreateLogger<KafkaConsumer>();
        if (config.EnableAutoCommit != true)
        {
            throw new ArgumentException("EnableAutoCommit must be true");
        }

        if (config.EnableAutoOffsetStore != false)
        {
            throw new ArgumentException("EnableAutoOffsetStore must be false");
        }

        _config = config;
        _topics = topics;
        _handleTopicPartition = handleTopicPartition;
    }

    private IConsumer<string, byte[]> BuildConsumer(CancellationToken gracefulShutdownToken)
    {
        return new ConsumerBuilder<string, byte[]>(_config).Build();
    }

    /// <summary>
    /// Begin consuming messages
    /// </summary>
    /// <param name="gracefulShutdownToken">Causes the consumer to stop consuming new messages. Does not cancel messages in flight</param>
    public void Consume(CancellationToken gracefulShutdownToken)
    {
        using var consumer = BuildConsumer(gracefulShutdownToken);
        _logger.LogInformation("Subscribing to {Topics}", string.Join(", ", _topics));
        consumer.Subscribe(_topics);
        while (!gracefulShutdownToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(100);
                gracefulShutdownToken.ThrowIfCancellationRequested();

                if (consumeResult == null) continue;

                _handleTopicPartition(consumeResult, consumer.StoreOffset, gracefulShutdownToken).Wait();
            }
            catch (OperationCanceledException) when (gracefulShutdownToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                gracefulShutdownToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));
                _logger.LogError(ex, "Error while consuming");
            }
        }

        // Calling "Close" invokes the partitions revoked handler which will wait for all inflight messaging
        // to complete processing
        consumer.Close();
    }
}