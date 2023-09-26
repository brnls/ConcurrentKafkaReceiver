using Confluent.Kafka;

const string topic = "final-topic";

string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

using var producer = new ProducerBuilder<string, string>(new Dictionary<string, string>
{
    { "bootstrap.servers", "localhost:9092" }
}).Build();

var numProduced = 0;
Random rnd = Random.Shared;
const int numMessages = 10;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    cts.Cancel();
    e.Cancel = true;
};

while (true)
{
    await Task.Delay(2, cts.Token);
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
}

producer.Flush(TimeSpan.FromSeconds(10));
Console.WriteLine($"{numProduced} messages were produced to topic {topic}");