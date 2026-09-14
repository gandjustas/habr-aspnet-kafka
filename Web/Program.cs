using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using ZeroAlloc.Outbox;
using ZeroAlloc.Outbox.EfCore;


var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

builder.AddNpgsqlDbContext<AppDbContext>("database",
    configureDbContextOptions: options => { 
        options.UseSnakeCaseNamingConvention();
    });


var kafkaSerializer = new KafkaJsonSerializer<Message>();
builder.AddKafkaProducer<int, Message>("kafka",
    configureBuilder: builder => {
        builder.SetValueSerializer(kafkaSerializer);
    });
builder.AddKafkaConsumer<int, Message>("kafka",
    configureBuilder: builder => {
        builder.SetValueDeserializer(kafkaSerializer);    
    });
builder.Services.AddHostedService<KafkaConsumer>();

builder.Services.AddOutbox(options =>
{
    options.PollingInterval = TimeSpan.FromMilliseconds(100);
    options.BatchSize = 50;
    options.MaxAttempts = 3;
})
.WithEfCore<AppDbContext>()
.AddMessageOutbox();

builder.Services.AddTransient<IOutboxDispatcher<Message>, OutboxDispatcher>();

builder.Services.AddKeyedSingleton("completions" ,(_,_) => new ConcurrentDictionary<int, TaskCompletionSource<Message>>());

builder.Services.AddKeyedSingleton("completions-replication", (_, _) => new ConcurrentDictionary<int, TaskCompletionSource<Message>>());
builder.Services.AddHostedService<PgOutputConsumerService>();

builder.AddKafkaConsumer<Ignore, string>("kafka",
    configureSettings: settings => settings.Config.GroupId = "debezium-consumer");
builder.Services.AddKeyedSingleton("completions-debezium", (_, _) => new ConcurrentDictionary<int, TaskCompletionSource<Message>>());
builder.Services.AddHostedService<DebeziumConsumer>();

var app = builder.Build();

await EnsureReplicationSetupAsync(app.Configuration);

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
    msg = await completions.WaitForCompletionAsync(msg.Id, ct);
    return Results.Created($"/messages/{msg.Id}", msg);
});

app.MapPost("/outbox", async (Message dto,
                               AppDbContext db,
                               IOutboxWriter <Message> outbox,
                               [FromKeyedServices("completions")] ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
                               CancellationToken ct) =>
{
    var id = await db.Database.CreateExecutionStrategy().ExecuteInTransactionAsync(async (ct) =>
    {
        Message msg = new()
        {
            Content = dto.Content,
            CreatedAt = DateTime.UtcNow,
        };
        db.Messages.Add(msg);
        await db.SaveChangesAsync(ct);
        await outbox.WriteAsync(msg, ct: ct);
        return msg.Id;
    }, ct => Task.FromResult(false), ct);

    var msg = await completions.WaitForCompletionAsync(id, ct);
    return Results.Created($"/messages/{id}", msg);
});

app.MapPost("/replication", async (Message dto,
                               AppDbContext db,
                               [FromKeyedServices("completions-replication")] ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
                               CancellationToken ct) =>
{
    Message msg = new()
    {
        Content = dto.Content,
        CreatedAt = DateTime.UtcNow,
    };
    db.Messages.Add(msg);
    await db.SaveChangesAsync(ct);

    // Отдельной отправки нет: строка уже попала в WAL как часть INSERT выше.
    msg = await completions.WaitForCompletionAsync(msg.Id, ct);
    return Results.Created($"/messages/{msg.Id}", msg);
});

app.MapPost("/debezium", async (Message dto,
                               AppDbContext db,
                               [FromKeyedServices("completions-debezium")] ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
                               CancellationToken ct) =>
{
    Message msg = new()
    {
        Content = dto.Content,
        CreatedAt = DateTime.UtcNow,
    };
    db.Messages.Add(msg);
    await db.SaveChangesAsync(ct);

    // Debezium сам читает WAL через отдельный слот/публикацию и публикует CDC-событие в Kafka.
    msg = await completions.WaitForCompletionAsync(msg.Id, ct);
    return Results.Created($"/messages/{msg.Id}", msg);
});


app.Run();

static async Task EnsureReplicationSetupAsync(IConfiguration configuration)
{
    var connectionString = configuration.GetConnectionString("database");

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    await using (var cmd = new NpgsqlCommand(
        $"""
         DO $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '{PgOutputConsumerService.PublicationName}') THEN
                 CREATE PUBLICATION {PgOutputConsumerService.PublicationName} FOR TABLE messages;
             END IF;
         END $$;
         """, conn))
    {
        await cmd.ExecuteNonQueryAsync();
    }

    await using (var cmd = new NpgsqlCommand(
        $"""
         DO $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{PgOutputConsumerService.SlotName}') THEN
                 PERFORM pg_create_logical_replication_slot('{PgOutputConsumerService.SlotName}', 'pgoutput');
             END IF;
         END $$;
         """, conn))
    {
        await cmd.ExecuteNonQueryAsync();
    }
}

[OutboxMessage]
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

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddOutboxMessages();
    }
}

public class OutboxDispatcher(IProducer<int, Message> producer) : IOutboxDispatcher<Message>
{
    public async ValueTask DispatchAsync(Message message, CancellationToken ct)
        => await producer.ProduceAsync(KafkaConsumer.Topic, new()
        {
            Timestamp = new(message.CreatedAt),
            Key = message.Id,
            Value = message
        }, ct);
}

