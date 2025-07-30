using Confluent.Kafka;

//string[] topics = ["topic-name", "batch-topic"];
//string[] topics = ["batch-topic"];
string[] topics = ["topic-name"];

string[] users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"];
string[] items = ["book", "alarm clock", "t-shirts", "gift card", "batteries"];

const string payload = """
        {
        "specversion" : "1.0",
        "type" : "com.example.someevent",
        "source" : "/mycontext",
        "subject": null,
        "id" : "D234-1234-1234",
        "time" : "2018-04-05T17:31:00Z",
        "comexampleextension1" : "value",
        "comexampleothervalue" : 5,
        "data" : "{\r\n    \"specversion\" : \"1.0\",\r\n    \"type\" : \"com.example.someevent\",\r\n    \"source\" : \"\/mycontext\",\r\n    \"subject\": null,\r\n    \"id\" : \"D234-1234-1234\",\r\n    \"time\" : \"2018-04-05T17:31:00Z\",\r\n    \"comexampleextension1\" : \"value\",\r\n    \"comexampleothervalue\" : 5,\r\n    \"data\" : \"I'm just a string\"\r\n{\r\n    \"specversion\" : \"1.0\",\r\n    \"type\" : \"com.example.someevent\",\r\n    \"source\" : \"\/mycontext\",\r\n    \"subject\": null,\r\n    \"id\" : \"D234-1234-1234\",\r\n    \"time\" : \"2018-04-05T17:31:00Z\",\r\n    \"comexampleextension1\" : \"value\",\r\n    \"comexampleothervalue\" : 5,\r\n    \"data\" : \"I'm just a string\"\r\n}}"
    }
    """;

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
        while (numProduced <= 99999)
        {
            cts.Token.ThrowIfCancellationRequested();
            var user = users[rnd.Next(users.Length)];
            var item = items[rnd.Next(items.Length)];
            producer.Produce(x, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = payload },
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