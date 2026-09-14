using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

// Читает Kafka-топик, в который Debezium публикует CDC-события из WAL Postgres
// (отдельные слот/публикация, независимые от PgOutputConsumerService).
public class DebeziumConsumer(
    IConsumer<Ignore, string> consumer,
    [FromKeyedServices("completions-debezium")] ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
    ILogger<DebeziumConsumer> logger)
    : BackgroundService
{
    public const string Topic = "cdc.public.messages";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

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
                    if (consumed.Message.Value is not null)
                    {
                        var envelope = JsonSerializer.Deserialize<DebeziumEnvelope>(consumed.Message.Value, JsonOptions);
                        if (envelope?.After is { } after)
                        {
                            completions.Complete(after.Id, new Message
                            {
                                Id = after.Id,
                                Content = after.Content,
                                CreatedAt = after.CreatedAt,
                            });
                        }
                    }
                    consumer.Commit(consumed);
                }
                consumer.Close();
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                logger.LogWarning(ex, "Debezium consumer failed, retrying");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private class DebeziumEnvelope
    {
        public DebeziumAfter? After { get; set; }
    }

    private class DebeziumAfter
    {
        public int Id { get; set; }
        public string Content { get; set; } = string.Empty;

        [JsonPropertyName("created_at")]
        public DateTime CreatedAt { get; set; }
    }
}
