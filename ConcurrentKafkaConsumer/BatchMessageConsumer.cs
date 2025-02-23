using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Brnls;

public class BatchMessageConsumer
{
    private readonly TopicPartitionConsumer _partitionConsumer;
    private readonly BatchMessageHandler _handler;
    private readonly int _maxBatch;
    private readonly ILogger<BatchMessageConsumer> _logger;
    private readonly List<ConsumeResult<string, byte[]>> _buffer;

    public BatchMessageConsumer(
        TopicPartitionConsumer partitionConsumer,
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
                ConsumerMetrics.BatchConsumerBatchSize.Record(_buffer.Count);
                await _handler(
                    _buffer,
                    StorePartialSuccessOffset,
                    _partitionConsumer.GracefulShutdownToken);
                _partitionConsumer.StoreOffset(_buffer[^1]);
                _buffer.Clear();
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
