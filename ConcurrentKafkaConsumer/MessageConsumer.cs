using Microsoft.Extensions.Logging;

namespace Brnls;

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
                await _handler(item, _partitionConsumer.GracefulShutdownToken);
                _partitionConsumer.MessageChanngel.TryRead(out var _);
                _partitionConsumer.StoreOffset(item);
            }
            catch (OperationCanceledException e) when (e.CancellationToken == _partitionConsumer.GracefulShutdownToken) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{TopicPartitionOffset} Uncaught exception while processing message.", item.TopicPartitionOffset);
                // We can't do anything useful here. The application message handler should be handling errors.
                // If it gets here, add a delay so we don't spin.
                await Task.Delay(TimeSpan.FromSeconds(30), _partitionConsumer.GracefulShutdownToken).ContinueWith(_ => { });
            }
        }
    }
}
