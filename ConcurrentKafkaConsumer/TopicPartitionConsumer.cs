﻿using System.Threading.Channels;
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
    private readonly Action _unpauseRequest;
    private readonly SemaphoreSlim _semaphore;
    private readonly Task _processTask;
    private readonly CancellationTokenSource _gracefulShutdownCts;
    private readonly CancellationTokenSource _ungraceulShutdownCts;
    public bool Paused { get; set; }

    public TopicPartitionConsumer(
        TopicPartition topicPartition,
        Channel<ConsumeResult<string, byte[]>> channel,
        CancellationToken stoppingToken,
        ILogger<TopicPartitionConsumer> logger,
        MessageHandler messageHandler,
        Action<ConsumeResult<string, byte[]>> storeOffset,
        Action unpauseRequest,
        SemaphoreSlim semaphore)
    {
        TopicPartition = topicPartition;
        _channel = channel;
        _logger = logger;
        _messageHandler = messageHandler;
        _storeOffset = storeOffset;
        _unpauseRequest = unpauseRequest;
        _semaphore = semaphore;
        _processTask = Task.Run(ProcessPartition);
        _gracefulShutdownCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _ungraceulShutdownCts = new CancellationTokenSource();
    }

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
        while (await _channel.Reader.WaitToReadAsync(_gracefulShutdownCts.Token))
        {
            if (!_channel.Reader.TryPeek(out var consumeResult)) continue;

            try
            {
                try
                {
                    await _semaphore.WaitAsync(_gracefulShutdownCts.Token);
                    _gracefulShutdownCts.Token.ThrowIfCancellationRequested();
                    _logger.LogDebug("Handling message {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                    await _messageHandler!(consumeResult, _ungraceulShutdownCts.Token);

                    _storeOffset(consumeResult);
                    _channel.Reader.TryRead(out _);

                    if (Paused && _channel.Reader.Count == 0)
                    {
                        _unpauseRequest();
                    }
                }
                finally
                {
                    _semaphore.Release();
                }
            }
            catch (OperationCanceledException) when (_gracefulShutdownCts.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{TopicPartitionOffset} Uncaught exception while handling message.", consumeResult.TopicPartitionOffset);
                // We can't do anything useful here. The application message handler should be handling errors.
                // If it gets here, add a delay so we don't spin.
                await Task.Delay(TimeSpan.FromSeconds(30), _gracefulShutdownCts.Token);
            }
        }
    }

    public void Dispose()
    {
        _gracefulShutdownCts.Dispose();
        _ungraceulShutdownCts.Dispose();
    }
}
