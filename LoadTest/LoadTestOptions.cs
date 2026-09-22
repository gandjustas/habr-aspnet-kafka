/// <summary>Настройки прогона: секция LoadTest в appsettings, переопределяются через --LoadTest:Key=Value.</summary>
public class LoadTestOptions
{
    public const string SectionName = "LoadTest";

    /// <summary>Какие плечи гонять: dbz-kafka, dbz-quorum, wal, wal-sharded.</summary>
    public string Transports { get; set; } = "dbz-kafka,dbz-quorum,wal,wal-sharded";

    /// <summary>Свип по числу получателей.</summary>
    public string Consumers { get; set; } = "1,2,4,8";

    public int Producers { get; set; } = 200;
    public int Messages { get; set; } = 100_000;
    public string Content { get; set; } = "helloworld";

    /// <summary>
    /// Прогревочный прогон в голове свипа: оплачивает JIT всего пути, пулы соединений и метаданные Kafka.
    /// Его результат помечается и в выводы не идёт - иначе штраф достался бы первому плечу свипа.
    /// </summary>
    public int WarmupMessages { get; set; } = 20_000;

    /// <summary>Партиций в топике Kafka на одного получателя.</summary>
    public int KafkaPartitionsPerConsumer { get; set; } = 2;

    public ushort RabbitPrefetch { get; set; } = 1000;

    /// <summary>Сколько сообщений получатель набирает перед тем, как одним запросом закрепить их в inbox.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Сколько ждать добора пачки, прежде чем закрепить её неполной.</summary>
    public int FlushDelayMs { get; set; } = 50;

    /// <summary>
    /// Имя топика, в который пишет Debezium Server: {topic.prefix}.{схема}.{таблица}. Приёмник его не
    /// настраивает, поэтому имя фиксированное, а изоляция прогонов держится на пересоздании топика.
    /// </summary>
    public string KafkaTopic { get; set; } = "dbzk.public.messages";

    /// <summary>Exchange, в который публикует Debezium Server; к нему привязывается quorum-очередь.</summary>
    public string Exchange { get; set; } = "load-dbz";

    public string QuorumQueue { get; set; } = "load-dbz-quorum";
    public string QuorumRoutingKey { get; set; } = "quorum";

    /// <summary>Имена ресурсов Aspire: по ним ищутся контейнеры, которые поднимаются на время своего прогона.</summary>
    public string KafkaResource { get; set; } = "dbz-kafka";
    public string QuorumResource { get; set; } = "dbz-quorum";

    /// <summary>Слоты Debezium Server: дропаются при остановке контейнера, но проверяем и добиваем сами.</summary>
    public string DebeziumSlotPrefix { get; set; } = "dbz_";
    public string KafkaSlot { get; set; } = "dbz_kafka_slot";
    public string QuorumSlot { get; set; } = "dbz_quorum_slot";

    /// <summary>Префикс слотов прямых читателей WAL: свой слот на каждого получателя.</summary>
    public string WalSlotPrefix { get; set; } = "load_test_slot";

    /// <summary>Публикация, которую читают и Debezium, и прямые читатели. Создаётся миграцией.</summary>
    public string Publication { get; set; } = "load_test_pub";

    /// <summary>Сколько ждать, пока CDC-читатель создаст слот и начнёт стримить.</summary>
    public int ReadyTimeoutSeconds { get; set; } = 120;

    public int DrainTimeoutSeconds { get; set; } = 600;

    /// <summary>Пауза между прогонами: quorum-очередь и Kafka должны успеть подчистить свои журналы.</summary>
    public int CooldownSeconds { get; set; } = 20;

    /// <summary>
    /// Шардов в режиме wal-sharded. Фиксировано и не зависит от числа получателей: шард - единица владения,
    /// как партиция, и при смене состава группы перераздаются шарды, а не пересчитывается разбиение.
    /// </summary>
    public int ShardCount { get; set; } = 64;

    /// <summary>Как часто получатель пересматривает владение шардами.</summary>
    public int ShardRefreshMs { get; set; } = 1000;

    public bool ConsoleMetrics { get; set; }
    public bool NBomberReports { get; set; } = true;
    public int SampleIntervalMs { get; set; } = 1000;
    public string? ResultsDir { get; set; }

    public IEnumerable<string> TransportList =>
        Transports.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

    public IEnumerable<int> ConsumerCounts =>
        Consumers.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).Select(int.Parse);

    public TimeSpan DrainTimeout => TimeSpan.FromSeconds(DrainTimeoutSeconds);
    public TimeSpan FlushDelay => TimeSpan.FromMilliseconds(FlushDelayMs);
    public TimeSpan ShardRefresh => TimeSpan.FromMilliseconds(ShardRefreshMs);
}
