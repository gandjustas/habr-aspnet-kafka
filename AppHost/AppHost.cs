using Google.Protobuf.WellKnownTypes;

var builder = DistributedApplication.CreateBuilder(args);


IResourceBuilder<PostgresServerResource> postgres = builder.AddPostgres("postgres")
    .WithArgs("-c", "wal_level=logical")
    .WithDataVolume();

var database = postgres.AddDatabase("database");

var kafka = builder.AddKafka("kafka")
    .WithDataVolume(); 

var debeziumConnect = builder.AddContainer("debezium-connect", "debezium/connect", "3.0.0.Final")
    .WithHttpEndpoint(targetPort: 8083, name: "api") // Expose Kafka Connect REST API
    .WithEnvironment("BOOTSTRAP_SERVERS", kafka)
    .WithEnvironment("GROUP_ID", "1")
    .WithEnvironment("CONFIG_STORAGE_TOPIC", "my_connect_configs")
    .WithEnvironment("OFFSET_STORAGE_TOPIC", "my_connect_offsets")
    .WithEnvironment("STATUS_STORAGE_TOPIC", "my_connect_statuses")
    .WaitFor(kafka)
    .WaitFor(database);


var web = builder.AddProject<Projects.Web>("web")
    .WithReference(database)
    .WithReference(kafka)
    .WithHttpEndpoint()
    .WaitFor(database)
    .WaitFor(kafka);
    ;

var migrations = web.AddEFMigrations("migrations")
    .WaitFor(database)
    .RunDatabaseUpdateOnStart();

web.WaitForCompletion(migrations);

builder.Build().Run();
