// See https://aka.ms/new-console-template for more information

using System.Net.Http.Json;
using System.Net.ServerSentEvents;

using CancellationTokenSource cts = new();
Console.CancelKeyPress += (s, e) =>
{
    Console.WriteLine("Canceling...");
    cts.Cancel();
    e.Cancel = true;
};

using HttpClient client = new();
try
{
    var msg = new HttpRequestMessage(HttpMethod.Post, "http://localhost:5000/sse")
    {
        Content = JsonContent.Create(new Dictionary<string, string>
        {
            ["queued.max.messages.kbytes"] = 1000000.ToString(),
            ["max.partition.fetch.bytes"] = 1_000_0000.ToString(),
            ["queued.min.messages"] = 5000.ToString(),
            ["tp.buffer"] = 20.ToString()
        })
    };

    var resp = await client.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, cts.Token);

    await foreach (SseItem<string> item in SseParser.Create(resp.Content.ReadAsStream()).EnumerateAsync(cts.Token))
    {
        Console.WriteLine(item.Data);
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Canceled");
}
catch (Exception e)
{
    Console.WriteLine(e);
}
