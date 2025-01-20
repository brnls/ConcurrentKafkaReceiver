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

public delegate Task BatchMessageHandler(
    IReadOnlyList<ConsumeResult<string, byte[]>> consumeResults,
    Action<ConsumeResult<string, byte[]>> storePartialSuccessOffset,
    CancellationToken cancellationToken);

public sealed class ConcurrentKafkaConsumer
{
    private readonly ConcurrentKafkaConsumerConfig _config;
    private readonly Action<string>? _statisticsHandler;
    private readonly Dictionary<string, Func<PartitionConsumer, Task>> _topics;
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly Dictionary<TopicPartition, PartitionConsumerHandle> _partitionConsumers = new();
    private readonly Channel<TopicPartition> _unpauseChannel = Channel.CreateUnbounded<TopicPartition>();

    public ConcurrentKafkaConsumer(
        ConcurrentKafkaConsumerConfig config,
        IEnumerable<TopicConfiguration> topics,
        ILoggerFactory loggerFactory,
        Action<string>? statisticsHandler = null)
    {
        _logger = loggerFactory.CreateLogger<ConcurrentKafkaConsumer>();
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

        _config = config;
        _statisticsHandler = statisticsHandler;
        _topics = topics.ToDictionary(x => x.Topic, x => x.TopicPartitionProcessor);
    }

    private IConsumer<string, byte[]> BuildConsumer(CancellationToken gracefulShutdownToken)
    {
        var builder = new ConsumerBuilder<string, byte[]>(_config.ConsumerConfig)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Assigned {TopicPartition}", topicPartition);
                    var consumerHandle = CreatePartitionConsumerHandle(c, topicPartition, gracefulShutdownToken);
                    _partitionConsumers[topicPartition] = consumerHandle;
                }
            })
            .SetPartitionsRevokedHandler((c, topicPartitions) =>
            {
                try
                {
                    foreach (var topicPartition in topicPartitions)
                    {
                        _logger.LogDebug("Revoked {TopicPartition}", topicPartition);
                    }


                    Task.WhenAll(topicPartitions.Select(
                        async x => {
                            try
                            {
                                await _partitionConsumers[x.TopicPartition].WaitForStop().WaitAsync(TimeSpan.FromSeconds(15));
                            }
                            catch (OperationCanceledException)
                            {
                                _logger.LogWarning("Timeout waiting for {TopicPartition} to stop processing", x.TopicPartition);
                            }
                        })).Wait();

                    foreach (var topicPartition in topicPartitions)
                    {
                        if (_partitionConsumers[topicPartition.TopicPartition].Paused)
                        {
                            // This partition is being revoked, but we need to unpause it so that
                            // if it gets reassigned to this consumer, processing continues.
                            c.Resume([topicPartition.TopicPartition]);
                        }
                        _partitionConsumers.Remove(topicPartition.TopicPartition);
                    }

                    _logger.LogDebug("Revoke partitions completed");
                }
                catch(Exception ex)
                {
                    _logger.LogError(ex, "Error while revoking partitions");
                }
            })
            .SetOffsetsCommittedHandler((c, off) =>
            {
                foreach (var com in off.Offsets)
                    _logger.LogDebug("Committing: {TopicPartitionOffset}", com);
            })
            .SetErrorHandler((c, e) =>
            {
                _logger.LogError(new KafkaException(e), e.Reason);
            })
            .SetLogHandler((c, l) =>
            {
                if((int)l.Level <= 4)
                {
                    _logger.LogError(l.Message);
                }
                else
                {
                    _logger.LogInformation(l.Message);
                }
            });

        if(_statisticsHandler != null)
        {
            builder.SetStatisticsHandler((c, stats) =>
                {
                    try
                    {
                        _statisticsHandler(stats);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in statistics handler");
                    }
                }
            );
        }

        return builder.Build();
    }

    /// <summary>
    /// Begin consuming messages
    /// </summary>
    /// <param name="gracefulShutdownToken">Causes the consumer to stop consuming new messages. Does not cancel messages in flight</param>
    public void Consume(CancellationToken gracefulShutdownToken)
    {
        using var consumer = BuildConsumer(gracefulShutdownToken);
        _logger.LogInformation("Subscribing to {Topics}", string.Join(", ", _topics.Keys));
        consumer.Subscribe(_topics.Keys);
        while (!gracefulShutdownToken.IsCancellationRequested)
        {
            try
            {
                while (!gracefulShutdownToken.IsCancellationRequested
                    && _unpauseChannel.Reader.TryRead(out var topicPartition)
                    && _partitionConsumers.TryGetValue(topicPartition, out var partitionConsumer))
                {
                    _logger.LogWarning("Resuming partition {TopicPartition}", topicPartition);

                    _partitionConsumers.Remove(topicPartition);
                    partitionConsumer.Dispose();
                    PartitionConsumerHandle consumerHandler = CreatePartitionConsumerHandle(consumer, topicPartition, gracefulShutdownToken);

                    _partitionConsumers[topicPartition] = consumerHandler;
                    consumer.Resume([topicPartition]);
                }

                var consumeResult = consumer.Consume(100);
                gracefulShutdownToken.ThrowIfCancellationRequested();

                if (consumeResult == null) continue;

                var topicPartitionConsumer = _partitionConsumers[consumeResult.TopicPartition];
                bool posted = topicPartitionConsumer.TryPostMessage(consumeResult);

                // The message will fail to post to the topic partition consumer if the bounded
                // channel has reached capacity. When this happens we need to pause the partition
                // to prevent unbounded memory usage. Note that this will also purge the underlying
                // librdkafka cache for this topic partition, causing messages to need to be refetched
                // from the broker once we are ready to receive more.
                if (!posted)
                {
                    topicPartitionConsumer.Pause();
                    _logger.LogWarning("Pausing partition {TopicPartition}", consumeResult.TopicPartition);
                    consumer.Pause([consumeResult.TopicPartition]);
                    consumer.Seek(consumeResult.TopicPartitionOffset);
                }
            }
            catch (OperationCanceledException) when (gracefulShutdownToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                gracefulShutdownToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));
                _logger.LogError(ex, "Error while consuming");
            }
        }

        // Closes invokes the partitions revoked handler which will wait for all inflight messaging
        // to complete processing
        consumer.Close();
    }

    private PartitionConsumerHandle CreatePartitionConsumerHandle(IConsumer<string, byte[]> consumer, TopicPartition topicPartition, CancellationToken gracefulShutdownToken)
    {
        var consumerHandler = new PartitionConsumerHandle(
            gracefulShutdownToken,
            consumeResult =>
            {
                consumer.StoreOffset(consumeResult);
                _logger.LogDebug("Stored {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
            },
            _topics[topicPartition.Topic]);

        consumerHandler.ProcessTask.ContinueWith(_ =>
        {
            if (gracefulShutdownToken.IsCancellationRequested || !consumerHandler.Paused) return;
            _unpauseChannel.Writer.TryWrite(topicPartition);
        }, TaskContinuationOptions.RunContinuationsAsynchronously);
        return consumerHandler;
    }

    class PartitionConsumerHandle : IDisposable
    {
        private readonly CancellationTokenSource _gracefulShutdownCts;
        private readonly Channel<ConsumeResult<string, byte[]>> _channel;

        public PartitionConsumerHandle(
            CancellationToken stoppingToken,
            Action<ConsumeResult<string, byte[]>> storeOffset,
            Func<PartitionConsumer, Task> partitionConsumerProcessPartition
            )
        {
            _gracefulShutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            _channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(new BoundedChannelOptions(1000)
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });

            PartitionConsumer = new PartitionConsumer(
                _gracefulShutdownCts.Token,
                _channel.Reader,
                storeOffset);

            ProcessTask = partitionConsumerProcessPartition(PartitionConsumer);
        }

        public PartitionConsumer PartitionConsumer { get; }

        public Task ProcessTask { get; }

        public bool TryPostMessage(ConsumeResult<string, byte[]> result)
        {
            return _channel.Writer.TryWrite(result);
        }

        public async Task WaitForStop()
        {
            _gracefulShutdownCts.Cancel();
            try
            {
                await ProcessTask;
            }
            catch (OperationCanceledException e) when (e.CancellationToken == _gracefulShutdownCts.Token) { }
        }

        /// <summary>
        /// Signals to this consumer that it should finish processing the messages currently in its channel
        /// and then stop.
        /// </summary>
        public void Pause()
        {
            _channel.Writer.TryComplete();
            Paused = true;
        }

        public bool Paused { get; private set; }

        public void Dispose()
        {
            _gracefulShutdownCts.Dispose();
        }
    }
}