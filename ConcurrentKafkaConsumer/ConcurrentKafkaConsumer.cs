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

public class TopicConfiguration
{
    public TopicConfiguration(
        string topic,
        Func<PartitionConsumer, Task> topicPartitionProcessor) 
    { 
        Topic = topic;
        TopicPartitionProcessor = topicPartitionProcessor;
    }

    public string Topic { get; }

    public Func<PartitionConsumer, Task> TopicPartitionProcessor { get; }

    public static TopicConfiguration MessageConsumer(
        string topic,
        ILoggerFactory loggerFactory,
        MessageHandler messageHandler)
    {
        return new TopicConfiguration(
            topic,
            partitionConsumer =>
            {
                return new MessageConsumer(
                    partitionConsumer,
                    messageHandler,
                    loggerFactory.CreateLogger<MessageConsumer>()).ProcessPartition();
            }
        );
    }

    public static TopicConfiguration BatchMessageConsumer(
        string topic,
        int maxMessages,
        ILoggerFactory loggerFactory,
        BatchMessageHandler messageHandler)
    {
        return new TopicConfiguration(
            topic,
            partitionConsumer =>
            {
                return new BatchMessageConsumer(
                    partitionConsumer,
                    messageHandler,
                    maxMessages,
                    loggerFactory.CreateLogger<BatchMessageConsumer>()).ProcessPartition();
            }
        );
    }
}

public sealed class ConcurrentKafkaConsumer
{
    private readonly ConcurrentKafkaConsumerConfig _config;
    private readonly Dictionary<string, Func<PartitionConsumer, Task>> _topics;
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
    private readonly Dictionary<TopicPartition, PartitionConsumerHandle> _partitionConsumers = new();
    private readonly Channel<TopicPartition> _unpauseChannel = Channel.CreateUnbounded<TopicPartition>();
    private readonly Channel<Task> _inflightMessageProcessing = Channel.CreateUnbounded<Task>();

    public ConcurrentKafkaConsumer(
        ConcurrentKafkaConsumerConfig config,
        IEnumerable<TopicConfiguration> topics,
        ILoggerFactory loggerFactory)
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
        _topics = topics.ToDictionary(x => x.Topic, x => x.TopicPartitionProcessor);
    }

    private IConsumer<string, byte[]> BuildConsumer(CancellationToken gracefulShutdownToken)
    {
        return new ConsumerBuilder<string, byte[]>(_config.ConsumerConfig)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Assigned {TopicPartition}", topicPartition);
                    var consumerHandler = new PartitionConsumerHandle(
                        gracefulShutdownToken,
                        consumeResult =>
                        {
                            c.StoreOffset(consumeResult);
                            _logger.LogDebug("Stored {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                        },
                        _topics[topicPartition.Topic]);
                    _partitionConsumers[topicPartition] = consumerHandler;
                }
            })
            .SetPartitionsRevokedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Revoked {TopicPartition}", topicPartition);
                }

                // Give currently in flight messages time to stop processing before cancelling
                foreach (var inflightProcessTask in topicPartitions.Select(
                    x => _partitionConsumers[x.TopicPartition].WaitForStop(default)))
                {
                    _inflightMessageProcessing.Writer.TryWrite(inflightProcessTask);
                }


                foreach (var topicPartition in topicPartitions)
                {
                    if (_partitionConsumers[topicPartition.TopicPartition].Paused)
                    {
                        // This partition is being revoked, but we need to unpause it so that
                        // if it gets reassigned to this consumer, processing continues.
                        c.Resume(new[] { topicPartition.TopicPartition });
                    }
                    _partitionConsumers.Remove(topicPartition.TopicPartition);
                }

                _logger.LogDebug("Revoke partitions completed");
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
            .Build();
    }

    /// <summary>
    /// Begin consuming messages. Use the cancellation tokens to stop consumption either gracefully
    /// or forcefully
    /// </summary>
    /// <param name="gracefulShutdownToken">Causes the consumer to stop consuming new messages</param>
    /// <param name="forcefulShutdownToken"></param>
    public void Consume(CancellationToken gracefulShutdownToken, CancellationToken forcefulShutdownToken)
    {
        using var consumer = BuildConsumer(gracefulShutdownToken);
        _logger.LogDebug("Subscribing to {Topics}", string.Join(", ", _topics));
        consumer.Subscribe(_topics.Keys);
        while (!gracefulShutdownToken.IsCancellationRequested)
        {
            while (!gracefulShutdownToken.IsCancellationRequested
                && _unpauseChannel.Reader.TryRead(out var topicPartition) 
                && _partitionConsumers.TryGetValue(topicPartition, out var partitionConsumer))
            {
                _logger.LogDebug("Resuming partition {TopicPartition}", topicPartition);
                consumer.Resume(new[] { topicPartition });
                partitionConsumer.Paused = false;
            }

            try
            {
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
                    _logger.LogDebug("Pausing partition {TopicPartition}", consumeResult.TopicPartition);
                    consumer.Pause(new[] { consumeResult.TopicPartition });
                    consumer.Seek(consumeResult.TopicPartitionOffset);
                    topicPartitionConsumer.Paused = true;
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

    class PartitionConsumerHandle : IDisposable
    {
        private readonly CancellationTokenSource _gracefulShutdownCts;
        private readonly CancellationTokenSource _forcefulShutdownCts;
        private readonly Channel<ConsumeResult<string, byte[]>> _channel;

        public PartitionConsumerHandle(
            CancellationToken stoppingToken,
            Action<ConsumeResult<string, byte[]>> storeOffset,
            Func<PartitionConsumer, Task> partitionConsumerProcessPartition
            )
        {
            _gracefulShutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _forcefulShutdownCts = new CancellationTokenSource();

            _channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(new BoundedChannelOptions(20)
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });

            PartitionConsumer = new PartitionConsumer(
                _gracefulShutdownCts.Token,
                _forcefulShutdownCts.Token,
                _channel.Reader,
                storeOffset);

            _processTask = partitionConsumerProcessPartition(PartitionConsumer);
        }

        public PartitionConsumer PartitionConsumer { get; }

        private readonly Task _processTask;

        public bool TryPostMessage(ConsumeResult<string, byte[]> result)
        {
            return _channel.Writer.TryWrite(result);
        }

        public async Task WaitForStop(CancellationToken token)
        {
            using var registration = token.Register(() => _forcefulShutdownCts.Cancel());
            _gracefulShutdownCts.Cancel();
            try
            {
                await _processTask;

            }
            catch (OperationCanceledException e) when (e.CancellationToken == _gracefulShutdownCts.Token) { }
        }

        public bool Paused { get; set; }

        public void Dispose()
        {
            _gracefulShutdownCts.Dispose();
            _forcefulShutdownCts.Dispose();
        }
    }
}

public class PartitionConsumer
{
    public PartitionConsumer(
        CancellationToken ungracefulShutdownToken,
        CancellationToken gracefulShutdownToken,
        ChannelReader<ConsumeResult<string, byte[]>> messageChanngel,
        Action<ConsumeResult<string, byte[]>> storeOffset)
    {
        UngracefulShutdownToken = ungracefulShutdownToken;
        GracefulShutdownToken = gracefulShutdownToken;
        MessageChanngel = messageChanngel;
        StoreOffset = storeOffset;
    }

    public CancellationToken UngracefulShutdownToken { get; }
    public CancellationToken GracefulShutdownToken { get; }
    public ChannelReader<ConsumeResult<string, byte[]>> MessageChanngel { get; }
    public Action<ConsumeResult<string, byte[]>> StoreOffset { get; }
}

class MessageConsumer
{
    private readonly PartitionConsumer _partitionConsumer;
    private readonly MessageHandler _handler;
    private readonly ILogger<MessageConsumer> _logger;

    public MessageConsumer(
        PartitionConsumer partitionConsumer,
        MessageHandler handler,
        ILogger<MessageConsumer> logger)
    {
        _partitionConsumer = partitionConsumer;
        _handler = handler;
        _logger = logger;
    }

    public async Task ProcessPartition()
    {

        while (await _partitionConsumer.MessageChanngel.WaitToReadAsync(_partitionConsumer.GracefulShutdownToken))
        {
            if (!_partitionConsumer.MessageChanngel.TryPeek(out var item)) continue;
            try
            {
                await _handler(item, _partitionConsumer.UngracefulShutdownToken);
                _partitionConsumer.MessageChanngel.TryRead(out var _);
                _partitionConsumer.StoreOffset(item);
            }
            catch (OperationCanceledException) when (_partitionConsumer.GracefulShutdownToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{TopicPartitionOffset} Uncaught exception while processing message.", item.TopicPartitionOffset);
                // We can't do anything useful here. The application message handler should be handling errors.
                // If it gets here, add a delay so we don't spin.
                await Task.Delay(TimeSpan.FromSeconds(30), _partitionConsumer.GracefulShutdownToken);
            }
        }
    }
}

class BatchMessageConsumer
{
    private readonly PartitionConsumer _partitionConsumer;
    private readonly BatchMessageHandler _handler;
    private readonly int _maxBatch;
    private readonly ILogger<BatchMessageConsumer> _logger;
    private readonly List<ConsumeResult<string, byte[]>> _buffer;

    public BatchMessageConsumer(
        PartitionConsumer partitionConsumer,
        BatchMessageHandler handler,
        int maxBatch,
        ILogger<BatchMessageConsumer> logger)
    {
        _partitionConsumer = partitionConsumer;
        _handler = handler;
        _maxBatch = maxBatch;
        _logger = logger;
        _buffer = new List<ConsumeResult<string, byte[]>>(maxBatch);
    }

    public async Task ProcessPartition()
    {
        while (_buffer.Count > 0 || await _partitionConsumer.MessageChanngel.WaitToReadAsync(_partitionConsumer.GracefulShutdownToken))
        {
            try
            {
                while (_buffer.Count < _maxBatch && _partitionConsumer.MessageChanngel.TryRead(out var item))
                {
                    _buffer.Add(item);
                }

                _partitionConsumer.GracefulShutdownToken.ThrowIfCancellationRequested();
                await _handler(
                    _buffer,
                    StorePartialSuccessOffset,
                    _partitionConsumer.UngracefulShutdownToken);
                _buffer.Clear();
                _partitionConsumer.StoreOffset(_buffer[^1]);
            }
            catch (OperationCanceledException) when (_partitionConsumer.GracefulShutdownToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{TopicPartitionOffset} Uncaught exception while processing message batch.", _buffer[0].TopicPartitionOffset);
                // We can't do anything useful here. The application message handler should be handling errors.
                // If it gets here, add a delay so we don't spin.
                await Task.Delay(TimeSpan.FromSeconds(30), _partitionConsumer.GracefulShutdownToken);
            }
        }
    }

    void StorePartialSuccessOffset(ConsumeResult<string, byte[]> cr)
    {
        var storedOffsetIndex = _buffer.FindIndex(c => c.Offset == cr.Offset);
        _buffer.RemoveRange(0, storedOffsetIndex + 1);
        _partitionConsumer.StoreOffset(cr);
    }
}
