using System.Collections.Generic;
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

public sealed class ConcurrentKafkaConsumer : IDisposable
{
    private readonly Dictionary<string, Func<PartitionConsumer, Task>> _topics;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<ConcurrentKafkaConsumer> _logger;
    private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
    private readonly CancellationToken _disposeCt;
    private readonly Dictionary<TopicPartition, PartitionConsumerHandle> _partitionConsumers = new();
    private readonly Channel<TopicPartition> _unpauseChannel = Channel.CreateUnbounded<TopicPartition>();

    public ConcurrentKafkaConsumer(
        ConcurrentKafkaConsumerConfig config,
        IEnumerable<TopicConfiguration> topics,
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

        _topics = topics.ToDictionary(x => x.Topic, x => x.TopicPartitionProcessor);
        _consumer = new ConsumerBuilder<string, byte[]>(config.ConsumerConfig)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Assigned {TopicPartition}", topicPartition);
                    var consumerHandler = new PartitionConsumerHandle(
                        _disposeCt,
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
                foreach(var topicPartition in topicPartitions)
                {
                    _logger.LogDebug("Revoked {TopicPartition}", topicPartition);
                }
                // Give currently in flight messages time to stop processing before cancelling
                var stopProcessingTokenSource = new CancellationTokenSource(config.GracefulShutdownTimeout);
                var stoppedProcessing = Task.WhenAll(topicPartitions.Select(
                    x => _partitionConsumers[x.TopicPartition].WaitForStop(stopProcessingTokenSource.Token)))
                    .Wait(config.GracefulShutdownTimeout);

                if (!stoppedProcessing)
                {
                    _logger.LogWarning("Timeout occurred while stopping one or more more topic partition consumers");
                }

                foreach(var topicPartition in topicPartitions)
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
                foreach(var com in off.Offsets)
                    _logger.LogDebug("Committing: {TopicPartitionOffset}", com);
            })
            .SetErrorHandler((c, e) =>
            {
                _logger.LogError(new KafkaException(e), e.Reason);
            })
            .Build();
    }

    /// <summary>
    /// Begin consuming messages. This method will not return until either the cancellation token
    /// is cancelled or this <see cref="ConcurrentKafkaConsumer"/> instance is disposed.
    /// </summary>
    /// <param name="messageHandler"></param>
    /// <param name="token"></param>
    public void Consume(CancellationToken token)
    {
        _logger.LogDebug("Subscribing to {Topics}", string.Join(", ", _topics));
        _consumer.Subscribe(_topics.Keys);
        using var consumeCts = CancellationTokenSource.CreateLinkedTokenSource(token, _disposeCt);
        while (!consumeCts.Token.IsCancellationRequested)
        {
            while (!consumeCts.Token.IsCancellationRequested
                && _unpauseChannel.Reader.TryRead(out var topicPartition) 
                && _partitionConsumers.TryGetValue(topicPartition, out var consumer))
            {
                _logger.LogDebug("Resuming partition {TopicPartition}", topicPartition);
                _consumer.Resume(new[] { topicPartition });
                consumer.Paused = false;
            }

            Console.WriteLine($"Running on thread {Thread.CurrentThread.ManagedThreadId}");
            try
            {
                var consumeResult = _consumer.Consume(100);
                consumeCts.Token.ThrowIfCancellationRequested();

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

    class PartitionConsumerHandle : IDisposable
    {
        private readonly CancellationTokenSource _gracefulShutdownCts;
        private readonly CancellationTokenSource _ungraceulShutdownCts;
        private readonly Channel<ConsumeResult<string, byte[]>> _channel;

        public PartitionConsumerHandle(
            CancellationToken stoppingToken,
            Action<ConsumeResult<string, byte[]>> storeOffset,
            Func<PartitionConsumer, Task> partitionConsumerProcessPartition
            )
        {
            _gracefulShutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _ungraceulShutdownCts = new CancellationTokenSource();

            _channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(new BoundedChannelOptions(20)
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });

            PartitionConsumer = new PartitionConsumer(
                _gracefulShutdownCts.Token,
                _ungraceulShutdownCts.Token,
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
            token.Register(() => _ungraceulShutdownCts.Cancel());
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
            _ungraceulShutdownCts.Dispose();
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
            if (!_partitionConsumer.MessageChanngel.TryPeek(out var item)) return;
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
    }
}
