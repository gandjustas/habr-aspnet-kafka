using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using static System.Runtime.InteropServices.JavaScript.JSType;


var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

builder.AddNpgsqlDbContext<AppDbContext>("database",
    configureDbContextOptions: options => { 
        options.UseSnakeCaseNamingConvention();
    });


var kafkaSerializer = new JsonSerializer<Message>();
builder.AddKafkaProducer<int, Message>("kafka",
    configureSettings: settings => {
        settings.Config.AllowAutoCreateTopics = true;
    },
    configureBuilder: builder => {
        builder.SetValueSerializer(kafkaSerializer);
    });
builder.AddKafkaConsumer<int, Message>("kafka",
    configureSettings: settings => { 
        settings.Config.AllowAutoCreateTopics = true;
    },
    configureBuilder: builder => {
        builder.SetValueDeserializer(kafkaSerializer);    
    });
builder.Services.AddHostedService<KafkaConsumer>();


builder.Services.AddKeyedSingleton("completions" ,(_,_) => new ConcurrentDictionary<int, TaskCompletionSource<Message>>());

var app = builder.Build();

// Configure the HTTP request pipeline.

app.MapPost("/direct", async (Message dto,
                               AppDbContext db,
                               CancellationToken ct) =>
{
    Message msg = new()
    {
        Content = dto.Content,
        CreatedAt = DateTime.UtcNow,
    };
    db.Messages.Add(msg);
    await db.SaveChangesAsync(ct);

    return Results.Created($"/messages/{msg.Id}", msg);
});

app.MapPost("/naive", async (Message dto,
                               AppDbContext db,
                               IProducer<int, Message> producer,
                               [FromKeyedServices("completions")]ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
                               CancellationToken ct) =>
{
    Message msg = new()
    {
        Content = dto.Content,
        CreatedAt = DateTime.UtcNow,
    };
    db.Messages.Add(msg);
    await db.SaveChangesAsync(ct);

    await producer.ProduceAsync(KafkaConsumer.Topic, new()
    {
        Timestamp = new(msg.CreatedAt),
        Key = msg.Id,
        Value = msg
    }, ct);
    msg = await completions.GetOrAdd(msg.Id, _ => new ()).Task.WaitAsync(ct);
    return Results.Created($"/messages/{msg.Id}", msg);
});


app.Run();

internal class JsonSerializer<T>: ISerializer<T>, IDeserializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data, JsonSerializerOptions.Web);
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) => isNull ? default : JsonSerializer.Deserialize<T>(data, JsonSerializerOptions.Web)!;
}

public class Message
{
    public int Id { get; set; }
    public string Content { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}


public class AppDbContext(DbContextOptions<AppDbContext> opts)
    : DbContext(opts)
{
    public DbSet<Message> Messages { get; set; }
}



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
                        try
                        {
                            var completion = completions.GetOrAdd(consumed.Message.Key, _ => new());
                            completion.TrySetResult(consumed.Message.Value);
                        }
                        finally
                        {
                            completions.TryRemove(consumed.Message.Key, out _);
                            consumer.Commit(consumed);
                        }
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