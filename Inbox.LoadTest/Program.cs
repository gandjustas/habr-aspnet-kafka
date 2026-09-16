using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NBomber.CSharp;

var builder = Host.CreateApplicationBuilder(args);
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
builder.AddRabbitMQClient("rmq");


builder.Services.AddSingleton<IProducerConsumerScenarios, KafkaScenarios>();
//builder.Services.AddSingleton<IProducerConsumerScenarios, RabbitMqScenarios>();
//builder.Services.AddSingleton<IProducerConsumerScenarios, PgReplicationScenarios>();

var host = builder.Build();

foreach (var s in host.Services.GetServices<IProducerConsumerScenarios>())
{
    NBomberRunner
        .RegisterScenarios(
            s.CreateProducerScenario(1, "helloworld", 100_000),
            s.CreateConsumerScenario(1, 100_000)
            )
        .WithTestSuite("Kafka vs Rabbit vs Logical Replication")
        .WithTestName(s.Name)
        .Run();
}

