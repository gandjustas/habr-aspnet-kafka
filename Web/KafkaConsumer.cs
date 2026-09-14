using System.Collections.Concurrent;
using Confluent.Kafka;

public class KafkaConsumer(
    IConsumer<int, Message> consumer,
    [FromKeyedServices("completions")]ConcurrentDictionary<int, TaskCompletionSource<Message>> completions)
    : BackgroundService
{
    public const string Topic = "messages";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                consumer.Subscribe(Topic);
                while (!stoppingToken.IsCancellationRequested)
                {
                        var consumed = consumer.Consume(stoppingToken);
                        completions.Complete(consumed.Message.Key, consumed.Message.Value);
                        consumer.Commit(consumed);
                }
                consumer.Close();
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}