using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using ZeroAlloc.Outbox;
using ZeroAlloc.Outbox.EfCore;


var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

builder.AddNpgsqlDbContext<AppDbContext>("database",
    configureSettings: settings =>
    {
        // Все вставки нагрузочного теста идут через этот пул, а отправителей 200
        settings.ConnectionString = new NpgsqlConnectionStringBuilder(settings.ConnectionString) { MaxPoolSize = 220 }.ConnectionString;
        settings.DisableTracing = true;
    },
    configureDbContextOptions: options => {
        options.UseSnakeCaseNamingConvention();
    });

builder.Services.AddSingleton<LoadStats>();

// Старый бенчмарк из статьи: свои фоновые консьюмеры, outbox и продюсер в Kafka. В сквозном замере они
// только мешают: слот rep_slot декодирует КАЖДУЮ транзакцию сервера, включая все вставки прогона,
// диспетчер outbox опрашивает базу каждые 100 мс, а продюсер держит соединение с брокером, который
// сейчас под замером. Поэтому по умолчанию всё это выключено.
var legacyConsumers = builder.Configuration.GetValue("Bench:LegacyConsumers", false);

if (legacyConsumers)
{
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
}

var app = builder.Build();

if (legacyConsumers) await EnsureReplicationSetupAsync(app.Configuration);

// Configure the HTTP request pipeline.

// Отправитель всех плеч: строка в базу и больше ничего. Дальше её разбирают Debezium и прямые
// читатели WAL - это и есть предмет замера. created_at ставит PostgreSQL, чтобы у всех плеч были
// одни часы: это же значение едет в WAL и в конверт Debezium.
app.MapPost("/load/messages", async (Message dto, AppDbContext db, CancellationToken ct) =>
{
    Message msg = new() { Content = dto.Content };
    db.Messages.Add(msg);
    await db.SaveChangesAsync(ct);

    return Results.Created($"/load/messages/{msg.Id}", new { msg.Id, msg.CreatedAt });
});

// Имитация полезной работы консьюмера: «отправка письма» ценой в переключение контекста
app.MapPost("/send-email", async (Message message, LoadStats stats) =>
{
    stats.Work(message.Id);
    await Task.Yield();
    return Results.Accepted();
});

app.MapGet("/load/stats", (LoadStats stats) => Results.Ok(stats.Snapshot()));

app.MapPost("/load/reset", (LoadStats stats) =>
{
    stats.Reset();
    return Results.NoContent();
});

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

if (legacyConsumers)
{
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
}


app.Run();

// Публикация rep_pub создаётся миграцией AddReplicationPublications, здесь остаётся только слот
static async Task EnsureReplicationSetupAsync(IConfiguration configuration)
{
    var connectionString = configuration.GetConnectionString("database");

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

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

