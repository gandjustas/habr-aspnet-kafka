var builder = DistributedApplication.CreateBuilder(args);

IResourceBuilder<PostgresServerResource> postgres = builder.AddPostgres("postgres")
    .WithArgs("-c", "wal_level=logical")
    .WithArgs("-c", "max_connections=300")
    .WithArgs("-c", "max_replication_slots=50")
    .WithArgs("-c", "idle_replication_slot_timeout=3600") // 1 час
    .WithArgs("-c", "max_slot_wal_keep_size=2048") // 2 GB
    .WithDataVolume();

var database = postgres.AddDatabase("database");

var kafka = builder.AddKafka("kafka")
    .WithDataVolume(); 

var rmq = builder.AddRabbitMQ("rmq").WithDataVolume();

// Веб поднимается сразу: нагрузочный тест зовёт его /send-email на каждое сообщение
var web = builder.AddProject<Projects.Web>("web")
    .WithReference(database)
    .WithReference(kafka)
    .WaitFor(database)
    .WaitFor(kafka);


// Значения берутся из конфигурации AppHost (секция Parameters), дефолт — если там ничего нет
var loadProducers = builder.AddParameter("load-producers", "200")
    .WithDescription("Сколько продюсеров поднимает нагрузочный тест");
var loadConsumers = builder.AddParameter("load-consumers", "1")
    .WithDescription("Сколько консьюмеров поднимает нагрузочный тест (партиций у топика будет вдвое больше)");
var loadMessages = builder.AddParameter("load-messages", "10000")
    .WithDescription("Сколько сообщений отправляет и ждёт нагрузочный тест");

var loadTest = builder.AddProject<Projects.LoadTest>("load-test")
    .WithReference(database)
    .WithReference(kafka)
    .WithReference(rmq)
    .WithReference(web)
    .WaitFor(web)
    .WaitFor(database)
    .WaitFor(rmq)
    .WaitFor(kafka)
    .WithEnvironment("LoadTest__Producers", loadProducers)
    .WithEnvironment("LoadTest__Consumers", loadConsumers)
    .WithEnvironment("LoadTest__MessageCount", loadMessages)
    .WithExplicitStart();

var migrations = web.AddEFMigrations("migrations")
    .WaitFor(database)
    .RunDatabaseUpdateOnStart();

web.WaitForCompletion(migrations);
// Публикация для логической репликации создаётся миграцией, тест без неё не стартует
loadTest.WaitForCompletion(migrations);

builder.Build().Run();
