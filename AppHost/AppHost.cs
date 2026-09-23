using Microsoft.Extensions.Configuration;

var builder = DistributedApplication.CreateBuilder(args);

// Фоновые консьюмеры старого бенчмарка держат слот, который декодирует каждую транзакцию сервера,
// поэтому в сквозном замере они выключены. Включаются через --Bench:LegacyConsumers=true.
var legacyConsumers = builder.Configuration.GetValue("Bench:LegacyConsumers", false);

// Пароли фиксированные: их должны знать не только .NET-клиенты, но и конфигурация Debezium Server,
// которая собирается текстом из тех же параметров. Случайный пароль Aspire туда не передать, а его
// спецсимволы ломали бы разбор properties.
var postgresPassword = builder.AddParameter("postgres-password", "postgres_demo_password", secret: true);
var rabbitUser = builder.AddParameter("rmq-user", "guest");
var rabbitPassword = builder.AddParameter("rmq-password", "rmq_demo_password", secret: true);

// Оба CDC-конвейера работают на одном движке одной версии: иначе в цифрах была бы разница движков,
// а не приёмников.
const string DebeziumVersion = "3.6.2.Final";

IResourceBuilder<PostgresServerResource> postgres = builder.AddPostgres("postgres", password: postgresPassword)
    .WithArgs("-c", "wal_level=logical")
    .WithArgs("-c", "max_connections=400") // пул веб-сервиса 220 + пулы теста + слоты репликации
    .WithArgs("-c", "max_replication_slots=64")
    .WithArgs("-c", "max_wal_senders=64") // 8 слотов wal-плеча + 2 Debezium + запас; дефолт 10 - ровно потолок
    .WithArgs("-c", "idle_replication_slot_timeout=3600") // 1 час
    .WithArgs("-c", "max_slot_wal_keep_size=4096") // 4 GB
    .WithArgs("-c", "max_wal_size=8GB") // иначе чекпоинт посреди прогона добавляет шум в замер
    .WithArgs("-c", "wal_sender_timeout=0") // получатель, задержавшийся на работе, не должен терять walsender
    .WithDataVolume();

var database = postgres.AddDatabase("database");

// Топик на каждый прогон создаёт сам тест, с нужным числом партиций: топик, созданный брокером по первому
// сообщению, получил бы одну партицию, и весь свип упёрся бы в одного получателя. Автосоздание при этом
// не выключить - на нём держится health check Aspire, который публикует в свой топик; вместо запрета тест
// проверяет, что каждому получателю досталась своя доля партиций.
var kafka = builder.AddKafka("kafka")
    .WithDataVolume();

var rmq = builder.AddRabbitMQ("rmq", userName: rabbitUser, password: rabbitPassword)
    .WithManagementPlugin()
    .WithDataVolume();

// Веб поднимается сразу: отправители зовут его /load/messages, получатели - /send-email
var web = builder.AddProject<Projects.Web>("web")
    .WithReference(database)
    // Конвейер измеряется сквозь этот сервис, поэтому спан на каждый из ста тысяч запросов только мешает
    .WithEnvironment("OTEL_TRACES_SAMPLER", "always_off")
    .WithEnvironment("Bench__LegacyConsumers", legacyConsumers.ToString())
    .WaitFor(database);

var migrations = web.AddEFMigrations("migrations")
    .WaitFor(database)
    .RunDatabaseUpdateOnStart();

web.WaitForCompletion(migrations);

// Общая часть конфигурации обоих Debezium Server: источник обязан быть одинаков, иначе сравнивались бы
// настройки, а не приёмники. Позиция хранится в памяти, слот дропается при остановке - после выключения
// контейнера состояния не остаётся вовсе, и следующий запуск начинается с чистого листа.
static string DebeziumSourceConfig(string prefix, string slot, string password) =>
    $"""
     debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
     debezium.source.offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore
     debezium.source.offset.flush.interval.ms=0
     debezium.source.database.hostname=postgres
     debezium.source.database.port=5432
     debezium.source.database.user=postgres
     debezium.source.database.password={password}
     debezium.source.database.dbname=database
     debezium.source.topic.prefix={prefix}
     debezium.source.table.include.list=public.messages
     debezium.source.plugin.name=pgoutput
     debezium.source.slot.name={slot}
     debezium.source.publication.name=load_test_pub
     debezium.source.publication.autocreate.mode=disabled
     debezium.source.snapshot.mode=no_data
     debezium.source.slot.drop.on.stop=true
     debezium.source.tombstones.on.delete=false
     debezium.source.skipped.operations=t,u,d
     debezium.source.max.batch.size=8192
     debezium.source.max.queue.size=32768
     debezium.source.poll.interval.ms=100
     debezium.format.key=json
     debezium.format.key.schemas.enable=false
     debezium.format.value=json
     debezium.format.value.schemas.enable=false
     quarkus.log.level=INFO
     """;

// Адрес брокера внутри сети контейнеров: снаружи Aspire анонсирует другой порт, и по нему Debezium
// до брокера не достучится.
var kafkaInternal = kafka.GetEndpoint("internal", KnownNetworkIdentifiers.DefaultAspireContainerNetwork)
    .Property(EndpointProperty.HostAndPort);

// Конвейеры Debezium поднимаются вручную: нагрузочный тест контейнерами не управляет, он только проверяет,
// что нужный слот стримит, и предупреждает, если журнал читает кто-то ещё. Перед прогоном в дашборде Aspire
// нужно поднять ровно тот конвейер, который меряется: работающий рядом второй декодирует те же вставки
// и ляжет в замер фоном.
var debeziumKafka = builder.AddContainer("dbz-kafka", "quay.io/debezium/server", DebeziumVersion)
    .WithEnvironment("JAVA_OPTS", "-Xms512m -Xmx1g") // паузы GC не должны выглядеть как задержка конвейера
    .WithContainerFiles("/debezium/config", async (_, ct) =>
    [
        new ContainerFile
        {
            Name = "application.properties",
            Contents =
            $"""
             debezium.sink.type=kafka
             debezium.sink.kafka.producer.bootstrap.servers={await ((IValueProvider)kafkaInternal).GetValueAsync(ct)}
             debezium.sink.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer
             debezium.sink.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
             debezium.sink.kafka.producer.acks=all
             debezium.sink.kafka.producer.linger.ms=10
             {DebeziumSourceConfig("dbzk", "dbz_kafka_slot", await postgresPassword.Resource.GetValueAsync(ct))}
             """,
        },
    ])
    .WaitFor(kafka)
    .WaitForCompletion(migrations) // публикацию load_test_pub создаёт миграция, Debezium её только читает
    .WithExplicitStart();

var debeziumQuorum = builder.AddContainer("dbz-quorum", "quay.io/debezium/server", DebeziumVersion)
    .WithEnvironment("JAVA_OPTS", "-Xms512m -Xmx1g")
    .WithContainerFiles("/debezium/config", async (_, ct) =>
    [
        new ContainerFile
        {
            Name = "application.properties",
            Contents =
            $"""
             debezium.sink.type=rabbitmq
             debezium.sink.rabbitmq.connection.host=rmq
             debezium.sink.rabbitmq.connection.port=5672
             debezium.sink.rabbitmq.connection.username={await rabbitUser.Resource.GetValueAsync(ct)}
             debezium.sink.rabbitmq.connection.password={await rabbitPassword.Resource.GetValueAsync(ct)}
             debezium.sink.rabbitmq.exchange=load-dbz
             debezium.sink.rabbitmq.routingKey=quorum
             debezium.sink.rabbitmq.routingKey.source=static
             debezium.sink.rabbitmq.autoCreateRoutingKey=false
             debezium.sink.rabbitmq.deliveryMode=2
             {DebeziumSourceConfig("dbzq", "dbz_quorum_slot", await postgresPassword.Resource.GetValueAsync(ct))}
             """,
        },
    ])
    .WaitFor(rmq)
    .WaitForCompletion(migrations)
    .WithExplicitStart();

// Свип целиком живёт внутри процесса теста: плечи и число получателей перебираются в цикле, чтобы
// прогрев JIT, пулов и метаданных не попадал в каждый прогон заново.
var loadTest = builder.AddProject<Projects.LoadTest>("load-test")
    .WithReference(database)
    .WithReference(kafka)
    .WithReference(rmq)
    .WithReference(web)
    .WithArgs("--LoadTest:Transports=dbz-kafka,dbz-quorum,wal,wal-sharded", "--LoadTest:Consumers=1,2,4,8")
    .WaitFor(web)
    .WaitFor(database)
    .WaitFor(rmq)
    .WaitFor(kafka)
    .WithExplicitStart();

// Публикация для логической репликации создаётся миграцией, тест без неё не стартует.
// WaitFor на контейнеры Debezium намеренно нет: они поднимаются вручную и только под свой прогон.
loadTest.WaitForCompletion(migrations);

builder.Build().Run();
