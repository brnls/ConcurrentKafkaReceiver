using Confluent.Kafka;

string[] topics = ["topic-name", "batch-topic"];
//string[] topics = ["batch-topic"];

string[] users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"];
string[] items = ["book", "alarm clock", "t-shirts", "gift card", "batteries"];

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    SecurityProtocol = SecurityProtocol.Plaintext
};

using var producer = new ProducerBuilder<string, string>(config).Build();

Random rnd = Random.Shared;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    cts.Cancel();
    e.Cancel = true;
};



var tasks = topics.Select(x => Task.Run(async () =>
{
    var numProduced = 0;
    try
    {
        while (numProduced <= 1000)
        {
            cts.Token.ThrowIfCancellationRequested();
            var user = users[rnd.Next(users.Length)];
            var item = items[rnd.Next(items.Length)];
            producer.Produce(x, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = item },
                (deliveryReport) =>
                {
                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                    {
                        Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                    }
                });
            numProduced += 1;
        }
    }
    catch { }
    finally
    {
        using var ctsFlush = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        producer.Flush(ctsFlush.Token);
        Console.WriteLine($"{numProduced} messages were produced to topic {x}");
    }
}));
await Task.WhenAll(tasks);