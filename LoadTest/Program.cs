using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EasyNetQ;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NBomber.CSharp;
using RabbitMQ.Client;
using Npgsql;
using Npgsql.Replication;

const string topic = "messages";
const string queue = "messages";
const string publication = "load_test_pub";
const string slot = "load_test_slot";

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

var producers = builder.Configuration.GetValue("LoadTest:Producers", 1);
var consumers = builder.Configuration.GetValue("LoadTest:Consumers", 1);
var messageCount = builder.Configuration.GetValue("LoadTest:MessageCount", 100_000);

builder.AddNpgsqlDbContext<AppDbContext>("database",
    configureDbContextOptions: options => {
        options.UseSnakeCaseNamingConvention();
    });
var kafkaSerializer = new KafkaJsonSerializer<Message>();
builder.AddKafkaProducer<int, Message>("kafka",
    configureBuilder: builder => {
        builder.SetValueSerializer(kafkaSerializer);
    });
builder.Services
    .Configure<ConsumerConfig>(builder.Configuration.GetSection("Aspire:Confluent:Kafka:Consumer:Config"))
    .PostConfigure<ConsumerConfig>(config =>
    {
        config.BootstrapServers = builder.Configuration.GetConnectionString("kafka");
        // Топик создаётся заново на каждый прогон, поэтому читаем его с нуля и своей группой,
        // чтобы не зацепить оффсеты предыдущих прогонов и не пропустить начало потока
        config.GroupId = $"{config.GroupId ?? "load-test"}-{Guid.NewGuid():N}";
        config.AutoOffsetReset = AutoOffsetReset.Earliest;
    });
builder.Services.AddTransient(sp =>
    new ConsumerBuilder<int, Message>(sp.GetRequiredService<IOptions<ConsumerConfig>>().Value)
        .SetValueDeserializer(kafkaSerializer)
        .Build());

builder.Services.AddSingleton(sp => new AdminClientConfig
{
    BootstrapServers = sp.GetRequiredService<IConfiguration>().GetConnectionString("kafka")
});
builder.Services.AddSingleton<IAdminClient>(sp =>
    new AdminClientBuilder(sp.GetRequiredService<AdminClientConfig>()).Build());

builder.Services.AddSingleton(sp =>
    NpgsqlDataSource.Create(sp.GetRequiredService<IConfiguration>().GetConnectionString("database")!));
builder.Services.AddTransient(sp =>
    new LogicalReplicationConnection(sp.GetRequiredService<IConfiguration>().GetConnectionString("database")));

// Консьюмеры дёргают веб-приложение на каждое сообщение. Устойчивость выключена: ретраи,
// таймауты и лимит параллельности из стандартного набора исказили бы замер
#pragma warning disable EXTEXP0001 // RemoveAllResilienceHandlers помечен экспериментальным
builder.Services.AddHttpClient(string.Empty, client => client.BaseAddress = new Uri("http://web"))
    .RemoveAllResilienceHandlers();
#pragma warning restore EXTEXP0001
builder.Services.AddSingleton(sp => sp.GetRequiredService<IHttpClientFactory>().CreateClient());

builder.AddRabbitMQClient("rmq");
builder.Services.AddEasyNetQ( builder.Configuration.GetConnectionString("rmq"));


builder.Services.AddKeyedSingleton<string>(KafkaScenarios.TopicKey, topic);
builder.Services.AddKeyedSingleton<string>(RabbitMqScenarios.QueueKey, queue);
builder.Services.AddKeyedSingleton<string>(PgReplicationScenarios.PublicationKey, publication);
builder.Services.AddKeyedSingleton<string>(PgReplicationScenarios.SlotKey, slot);
// Порядок регистрации задаёт порядок прогонов: кафка, кролик, репликация — три сеанса NBomber подряд
builder.Services.AddSingleton<IProducerConsumerScenarios, KafkaScenarios>();
builder.Services.AddSingleton<IProducerConsumerScenarios, RabbitMqScenarios>();
builder.Services.AddSingleton<IProducerConsumerScenarios, PgReplicationScenarios>();

using var host = builder.Build();

var admin = host.Services.GetRequiredService<IAdminClient>();

try
{
    await admin.CreateTopicsAsync([new TopicSpecification
    {
        Name = topic,
        // Партиций вдвое больше, чем консьюмеров: каждому достаётся по две
        NumPartitions = consumers * 2,
        ReplicationFactor = 1
    }]);
}
catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
{
    Console.WriteLine($"Топик {topic} уже существует");
}

// Очередь — тот же одноразовый ресурс прогона, что топик у кафки.
// Quorum-очередь реплицируется через Raft и пишет каждое сообщение на диск, как это делает кафка
var rabbit = host.Services.GetRequiredService<IConnection>();
await using var rabbitChannel = await rabbit.CreateChannelAsync();
await rabbitChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
    arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });

try
{
    foreach (var s in host.Services.GetServices<IProducerConsumerScenarios>())
    {
        NBomberRunner
            .RegisterScenarios(
                s.CreateProducerScenario(producers, "helloworld", messageCount),
                s.CreateConsumerScenario(consumers, messageCount)
                )
            .WithTestSuite("Kafka vs Rabbit vs Logical Replication")
            .WithTestName(s.Name)
            .Run();
    }
}
finally
{
    await admin.DeleteTopicsAsync([topic]);
    await rabbitChannel.QueueDeleteAsync(queue);
}

