using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Brnls;

public delegate Task MessageHandler(ConsumeResult<string, byte[]> result, CancellationToken cancellationToken);

public class ConcurrentKafkaReceiver : IDisposable
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IEnumerable<string> _topics;
    private readonly ILogger<ConcurrentKafkaReceiver> _logger;
    private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
    private readonly CancellationToken _disposeCt;
    private readonly Dictionary<TopicPartition, TopicPartitionConsumer> _topicPartitionConsumers = new();
    private MessageHandler? _messageHandler;
    private readonly Channel<TopicPartition> _unpauseChannel = Channel.CreateUnbounded<TopicPartition>();

    public ConcurrentKafkaReceiver(
        ConsumerConfig config,
        IEnumerable<string> topics,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ConcurrentKafkaReceiver>();
        _disposeCt = _disposeCts.Token;
        _consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetPartitionsAssignedHandler((c, topicPartitions) =>
            {
                foreach (var topicPartition in topicPartitions)
                {
                    _logger.LogInformation("Assigned partition {TopicPartition}", topicPartition);
                    _topicPartitionConsumers[topicPartition] = new TopicPartitionConsumer(
                        topicPartition,
                        Channel.CreateBounded<ConsumeResult<string, byte[]>>(
                        new BoundedChannelOptions(100)
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
                            _logger.LogInformation("Stored partition offset {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                        });
                }
            })
            .SetPartitionsRevokedHandler((c, topicPartitions) =>
            {
                foreach(var topicPartition in topicPartitions)
                {
                    _logger.LogInformation("Revoked partition {TopicPartition}", topicPartition);
                }
                // Give currently in flight messages 10 seconds to stop processing before cancelling
                var stopProcessingTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var stoppedProcessing = Task.WhenAll(topicPartitions.Select(
                    x => _topicPartitionConsumers[x.TopicPartition].WaitForStop(stopProcessingTokenSource.Token)))
                    .Wait(TimeSpan.FromSeconds(10));

                if (!stoppedProcessing)
                {
                    _logger.LogWarning("Timeout occurred while stopping one or more more topic partition consumers");
                }

                foreach(var topicPartition in topicPartitions)
                {
                    _topicPartitionConsumers.Remove(topicPartition.TopicPartition);
                }

                _logger.LogInformation("Revoke partitions completed");
            })
            .SetOffsetsCommittedHandler((c, off) =>
            {
                foreach(var com in off.Offsets)
                    _logger.LogInformation("Committing offset: {com}", com);
            })
            .Build();
        _topics = topics;
    }

    public async Task Receive(MessageHandler messageHandler, CancellationToken token)
    {
        _messageHandler = messageHandler;
        _ = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
            while (await timer.WaitForNextTickAsync(_disposeCt))
            {
                foreach (var topicPartitionConsumer in _topicPartitionConsumers.Values)
                {
                    if (topicPartitionConsumer.Messages == 0 && topicPartitionConsumer.Paused)
                    {
                        await _unpauseChannel.Writer.WriteAsync(topicPartitionConsumer.TopicPartition);
                    }
                }
            }
        });

        using var stopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
        _logger.LogInformation("Subscribing to {Topics}", string.Join(", ", _topics));
        _consumer.Subscribe(_topics);
        while (!stopTokenSource.Token.IsCancellationRequested)
        {
            while (_unpauseChannel.Reader.TryRead(out var topicPartition))
            {
                _logger.LogInformation("Resuming partition {TopicPartition}", topicPartition);
                _consumer.Resume(new[] { topicPartition });
            }

            try
            {
                var consumeResult = _consumer.Consume(100);
                stopTokenSource.Token.ThrowIfCancellationRequested();

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
                await Task.Delay(TimeSpan.FromSeconds(5), stopTokenSource.Token);
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
