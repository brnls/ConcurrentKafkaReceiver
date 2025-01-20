using Confluent.Kafka;

const string topic = "topic-name";

string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.Plaintext 
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var numProduced = 0;
Random rnd = Random.Shared;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    cts.Cancel();
    e.Cancel = true;
};

try
{
    while (true)
    {
        await Task.Delay(2, cts.Token);

        var i = 0;
        while (i < 10)
        {
            cts.Token.ThrowIfCancellationRequested();
            var user = users[rnd.Next(users.Length)];
            var item = items[rnd.Next(items.Length)];
            producer.Produce(topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = item },
                (deliveryReport) =>
                {
                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                    {
                        Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                    }
                    else
                    {
                        //Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                        numProduced += 1;
                    }
                });
            i++;
        }
    }
}
catch { }
finally
{
    producer.Flush();
}

Console.WriteLine($"{numProduced} messages were produced to topic {topic}");