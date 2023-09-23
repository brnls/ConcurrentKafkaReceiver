using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Brnls;

internal class TopicPartitionConsumer : IDisposable
{
    public TopicPartition TopicPartition { get; }
    private readonly Channel<ConsumeResult<string, byte[]>> _channel;
    private readonly ILogger<TopicPartitionConsumer> _logger;
    private readonly MessageHandler _messageHandler;
    private readonly Action<ConsumeResult<string, byte[]>> _storeOffset;
    private readonly Task _processTask;
    private readonly CancellationTokenSource _gracefulShutdownCts;
    private readonly CancellationTokenSource _ungraceulShutdownCts;
    public int Messages => _channel.Reader.Count;
    public bool Paused { get; set; }

    public TopicPartitionConsumer(
        TopicPartition topicPartition,
        Channel<ConsumeResult<string, byte[]>> channel,
        CancellationToken stoppingToken,
        ILogger<TopicPartitionConsumer> logger,
        MessageHandler messageHandler,
        Action<ConsumeResult<string, byte[]>> storeOffset)
    {
        TopicPartition = topicPartition;
        _channel = channel;
        _logger = logger;
        _messageHandler = messageHandler;
        _storeOffset = storeOffset;
        _processTask = Task.Run(ProcessPartition);
        _gracefulShutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _ungraceulShutdownCts = new CancellationTokenSource();
    }

    public TopicPartitionOffset? LatestOffset { get; private set; }

    public bool TryPostMessage(ConsumeResult<string, byte[]> result)
    {
        return _channel.Writer.TryWrite(result);
    }

    /// <summary>
    /// Stop processing the partition, waiting for the currently procesing message to complete.
    /// </summary>
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

    private async Task ProcessPartition()
    {
        while (true)
        {
            if (await _channel.Reader.WaitToReadAsync(_gracefulShutdownCts.Token))
            {
                if (!_channel.Reader.TryPeek(out var consumeResult)) continue;

                _logger.LogInformation("{TopicPartition} - Handling message offset {Offset}", TopicPartition, consumeResult.Offset);
                try
                {
                    await _messageHandler!(consumeResult, _ungraceulShutdownCts.Token);

                    LatestOffset = consumeResult.TopicPartitionOffset;
                    _storeOffset(consumeResult);
                    _channel.Reader.TryRead(out _);
                }
                catch (Exception ex)
                {
                    // message handler should be handling exceptions. If it gets here, add delay so we don't spin
                    await Task.Delay(1000, _gracefulShutdownCts.Token);
                    _logger.LogError(ex, "{TopicPartition} Uncaught exception while handling message.", TopicPartition);
                }
            }
        }
    }

    public void Dispose()
    {
        _gracefulShutdownCts.Dispose();
        _ungraceulShutdownCts.Dispose();
    }
}
