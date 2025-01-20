using Microsoft.Extensions.Logging;

namespace Brnls;

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
