using System.Runtime.InteropServices;

var builder = DistributedApplication.CreateBuilder(args);

// Фиксированный пароль нужен для того, чтобы load-test скрипт (k6) мог сам
// зарегистрировать/удалить Debezium-коннектор через REST API Kafka Connect,
// не вытаскивая случайно сгенерированный пароль из контейнера.
var postgresPassword = builder.AddParameter("postgres-password", "postgres_demo_password", secret: true);

IResourceBuilder<PostgresServerResource> postgres = builder.AddPostgres("postgres", password: postgresPassword)
    .WithArgs("-c", "wal_level=logical")
    .WithArgs("-c", "max_connections=300")
    .WithArgs("-c", "max_replication_slots=50")
    .WithArgs("-c", "idle_replication_slot_timeout=3600") // 1 час
    .WithArgs("-c", "max_slot_wal_keep_size=2048") // 2 GB
    .WithDataVolume();

var database = postgres.AddDatabase("database");

var kafka = builder.AddKafka("kafka")
    .WithDataVolume(); 

var debeziumConnect = builder.AddContainer("debezium-connect", "debezium/connect", "3.0.0.Final")
    .WithReference(kafka)
    .WithHttpEndpoint(port: 8083, targetPort: 8083, name: "api") // Expose Kafka Connect REST API on a stable host port
    .WithEnvironment("BOOTSTRAP_SERVERS", kafka.GetEndpoint("internal", KnownNetworkIdentifiers.DefaultAspireContainerNetwork))
    .WithEnvironment("GROUP_ID", "1")
    .WithEnvironment("CONFIG_STORAGE_TOPIC", "my_connect_configs")
    .WithEnvironment("OFFSET_STORAGE_TOPIC", "my_connect_offsets")
    .WithEnvironment("STATUS_STORAGE_TOPIC", "my_connect_statuses")
    .WaitFor(kafka)
    .WaitFor(database);

var rmq = builder.AddRabbitMQ("rmq");

var web = builder.AddProject<Projects.Web>("web")
    .WithReference(database)
    .WithReference(kafka)
    .WithHttpEndpoint(port: 5280, name: "http")
    .WaitFor(database)
    .WaitFor(kafka)
    .WithExplicitStart();


var inboxLoadTest = builder.AddProject<Projects.Inbox_LoadTest>("inbox-load")
    .WithReference(database)
    .WithReference(kafka)
    .WithReference(rmq)
    .WaitFor(database)
    .WaitFor(rmq)
    .WaitFor(kafka)
    .WithExplicitStart();

var migrations = web.AddEFMigrations("migrations")
    .WaitFor(database)
    .RunDatabaseUpdateOnStart();

web.WaitForCompletion(migrations);

builder.Build().Run();
