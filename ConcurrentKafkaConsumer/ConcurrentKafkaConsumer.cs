using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Brnls;

/// <summary>
/// The handler that will be invoked for each message consumed from
/// kafka. The cancellation token passed to this method will be cancelled
/// only after the <see cref="ConcurrentKafkaConsumerConfig.GracefulShutdownTimeout"/>
/// has elapsed to allow completing processing for currently in-flight messages.
/// </summary>
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
    private readonly SemaphoreSlim _semaphore;

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
        _semaphore = new SemaphoreSlim(config.MaxConcurrency);

        _consumer = new ConsumerBuilder<string, byte[]>(config.ConsumerConfig)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Assigned {TopicPartition}", topicPartition);
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
                            _logger.LogDebug("Stored {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                        },
                        () => { _unpauseChannel.Writer.TryWrite(topicPartition); },
                        _semaphore);
                }
            })
            .SetPartitionsRevokedHandler((c, topicPartitions) =>
            {
                foreach(var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Revoked {TopicPartition}", topicPartition);
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
                        // This partition is being revoked, but we need to unpause it so that
                        // if it gets reassigned to this consumer, processing continues.
                        c.Resume(new[] { topicPartition.TopicPartition });
                    }
                    _topicPartitionConsumers.Remove(topicPartition.TopicPartition);
                }

                _logger.LogDebug("Revoke partitions completed");
            })
            .SetOffsetsCommittedHandler((c, off) =>
            {
                foreach(var com in off.Offsets)
                    _logger.LogDebug("Committing: {TopicPartitionOffset}", com);
            })
            .SetErrorHandler((c, e) =>
            {
                _logger.LogError(new KafkaException(e), e.Reason);
            })
            .Build();
        _topics = topics;
    }

    /// <summary>
    /// Begin consuming messages. This method will not return until either the cancellation token
    /// is cancelled or this <see cref="ConcurrentKafkaConsumer"/> instance is disposed.
    /// </summary>
    /// <param name="messageHandler"></param>
    /// <param name="token"></param>
    public void Consume(MessageHandler messageHandler, CancellationToken token)
    {
        _messageHandler = messageHandler;

        _logger.LogDebug("Subscribing to {Topics}", string.Join(", ", _topics));
        _consumer.Subscribe(_topics);
        using var consumeCts = CancellationTokenSource.CreateLinkedTokenSource(token, _disposeCt);
        while (!consumeCts.Token.IsCancellationRequested)
        {
            while (!consumeCts.Token.IsCancellationRequested
                && _unpauseChannel.Reader.TryRead(out var topicPartition) 
                && _topicPartitionConsumers.TryGetValue(topicPartition, out var consumer))
            {
                _logger.LogDebug("Resuming partition {TopicPartition}", topicPartition);
                _consumer.Resume(new[] { topicPartition });
                consumer.Paused = false;
            }

            try
            {
                var consumeResult = _consumer.Consume(100);
                consumeCts.Token.ThrowIfCancellationRequested();

                if (consumeResult == null) continue;

                var topicPartitionConsumer = _topicPartitionConsumers[consumeResult.TopicPartition];
                bool posted = topicPartitionConsumer.TryPostMessage(consumeResult);
                // The message will fail to post to the topic partition consumer if the bounded
                // channel has reached capacity. When this happens we need to pause the partition
                // to prevent unbounded memory usage. Note that this will also purge the underlying
                // librdkafka cache for this topic partition, causing messages to need to be refetched
                // from the broker once we are ready to receive more.
                if (!posted)
                {
                    _logger.LogDebug("Pausing partition {TopicPartition}", consumeResult.TopicPartition);
                    _consumer.Pause(new[] { consumeResult.TopicPartition });
                    _consumer.Seek(consumeResult.TopicPartitionOffset);
                    topicPartitionConsumer.Paused = true;
                }
            }
            catch (OperationCanceledException) when (consumeCts.Token.IsCancellationRequested) { }
            catch (Exception ex)
            {
                consumeCts.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));
                _logger.LogError(ex, "Error while consuming");
            }
        }
    }

    /// <summary>
    /// Disposes of the kafka consumer. This method will not return until either all
    /// inflight messages have been processed or the <see cref="ConcurrentKafkaConsumerConfig.GracefulShutdownTimeout"/>
    /// has elapsed.
    /// </summary>
    public void Dispose()
    {
        _disposeCts.Cancel();
        _disposeCts.Dispose();

        // Calling close on the consumer will trigger the SetPartitionsRevokedHandler allowing
        // messages in flight time to gracefully complete processing.
        _consumer.Close();
        _consumer.Dispose();
    }
}
