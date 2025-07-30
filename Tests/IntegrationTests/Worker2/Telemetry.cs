using System.Diagnostics.Metrics;

namespace Worker2;
public class Telemetry
{
    private static readonly Meter _meter;
    private static readonly ObservableGauge<double> _processed;

    static Telemetry()
    {
        _meter = new Meter("Worker");
        _processed = _meter.CreateObservableGauge("items.processed.per_second", GetItemsPerSecond);
    }


    private static long _processedCount = 0;
    private static DateTime _lastUpdate = DateTime.UtcNow;

    public static void RecordProcessedItems(int count)
    {
        Interlocked.Add(ref _processedCount, count);
    }

    public static double GetItemsPerSecond()
    {
        var now = DateTime.UtcNow;
        var elapsedSeconds = (now - _lastUpdate).TotalSeconds;

        if (elapsedSeconds <= 0)
            return 0;

        long count = Interlocked.Exchange(ref _processedCount, 0);
        _lastUpdate = now;

        return count / elapsedSeconds;
    }
}
