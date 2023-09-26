using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Brnls;

public delegate Task MessageHandler(ConsumeResult<string, byte[]> result, CancellationToken cancellationToken);

public sealed class ConcurrentKafkaConsumer : IDisposable
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IEnumerable<string> _topics;
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
    private readonly CancellationToken _disposeCt;
    private readonly Dictionary<TopicPartition, TopicPartitionConsumer> _topicPartitionConsumers = new();
    private MessageHandler? _messageHandler;
    private readonly Channel<TopicPartition> _unpauseChannel = Channel.CreateUnbounded<TopicPartition>();

    public ConcurrentKafkaConsumer(
        ConcurrentKafkaConsumerConfig config,
        IEnumerable<string> topics,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ConcurrentKafkaConsumer>();
        _disposeCt = _disposeCts.Token;
        if (config.ConsumerConfig.EnableAutoCommit != true)
        {
            throw new ArgumentException("EnableAutoCommit must be true");
        }

        if (config.ConsumerConfig.EnableAutoOffsetStore != false)
        {
            throw new ArgumentException("EnableAutoOffsetStore must be false");
        }

        if (config.ConsumerConfig.PartitionAssignmentStrategy != PartitionAssignmentStrategy.CooperativeSticky)
        {
            throw new ArgumentException("PartitionAssignmentStrategy must be CooperativeSticky");
        }

        _consumer = new ConsumerBuilder<string, byte[]>(config.ConsumerConfig)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogInformation("Assigned {TopicPartition}", topicPartition);
                    _topicPartitionConsumers[topicPartition] = new TopicPartitionConsumer(
                        topicPartition,
                        Channel.CreateBounded<ConsumeResult<string, byte[]>>(
                        new BoundedChannelOptions(20)
                        {
                            SingleReader = true,
                            SingleWriter = true,
                            AllowSynchronousContinuations = false,
                        }),
                        _disposeCt,
                        loggerFactory.CreateLogger<TopicPartitionConsumer>(),
                        _messageHandler!,
                        consumeResult => 
                        {
                            c.StoreOffset(consumeResult); 
                            _logger.LogInformation("Stored {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                        },
                        () => { _unpauseChannel.Writer.TryWrite(topicPartition); });
                }
            })
            .SetPartitionsRevokedHandler((c, topicPartitions) =>
            {
                foreach(var topicPartition in topicPartitions)
                {
                    _logger.LogInformation("Revoked {TopicPartition}", topicPartition);
                }
                // Give currently in flight messages time to stop processing before cancelling
                var stopProcessingTokenSource = new CancellationTokenSource(config.GracefulShutdownTimeout);
                var stoppedProcessing = Task.WhenAll(topicPartitions.Select(
                    x => _topicPartitionConsumers[x.TopicPartition].WaitForStop(stopProcessingTokenSource.Token)))
                    .Wait(config.GracefulShutdownTimeout);

                if (!stoppedProcessing)
                {
                    _logger.LogWarning("Timeout occurred while stopping one or more more topic partition consumers");
                }

                foreach(var topicPartition in topicPartitions)
                {
                    if (_topicPartitionConsumers[topicPartition.TopicPartition].Paused)
                    {
                        // Unpause this partition so if it gets reassigned to this consumer, processing continues
                        c.Resume(new[] { topicPartition.TopicPartition });
                    }
                    _topicPartitionConsumers.Remove(topicPartition.TopicPartition);
                }

                _logger.LogInformation("Revoke partitions completed");
            })
            .SetOffsetsCommittedHandler((c, off) =>
            {
                foreach(var com in off.Offsets)
                    _logger.LogInformation("Committing: {TopicPartitionOffset}", com);
            })
            .Build();
        _topics = topics;
    }

    public async Task Consume(MessageHandler messageHandler, CancellationToken token)
    {
        await Task.Yield();
        _messageHandler = messageHandler;

        _logger.LogInformation("Subscribing to {Topics}", string.Join(", ", _topics));
        _consumer.Subscribe(_topics);
        while (!token.IsCancellationRequested)
        {
            while (_unpauseChannel.Reader.TryRead(out var topicPartition) 
                && _topicPartitionConsumers.TryGetValue(topicPartition, out var consumer))
            {
                _logger.LogInformation("Resuming partition {TopicPartition}", topicPartition);
                _consumer.Resume(new[] { topicPartition });
                consumer.Paused = false;
            }

            try
            {
                var consumeResult = _consumer.Consume(100);
                token.ThrowIfCancellationRequested();

                if (consumeResult is not null)
                {
                    var topicPartitionConsumer = _topicPartitionConsumers[consumeResult.TopicPartition];
                    bool posted = topicPartitionConsumer.TryPostMessage(consumeResult);
                    if (!posted)
                    {
                        _logger.LogInformation("Pausing partition {TopicPartition}", consumeResult.TopicPartition);
                        _consumer.Pause(new[] { consumeResult.TopicPartition });
                        _consumer.Seek(consumeResult.TopicPartitionOffset);
                        topicPartitionConsumer.Paused = true;
                    }
                }
            }
            catch (ConsumeException ex)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), token);
                _logger.LogError(ex, "Error while consuming");
            }
        }
    }

    public void Dispose()
    {
        _disposeCts.Cancel();
        _disposeCts.Dispose();
        _consumer.Close();
        _consumer.Dispose();
        _logger.LogInformation("consumer closed and disposed");
    }
}
