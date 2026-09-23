using System.Net.Http.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;

/// <summary>
/// Debezium Server читает логическую репликацию и публикует строки в топик Kafka, а получатели живут внутри
/// теста: набирают пачку, одним запросом закрепляют её в инбоксе и двигают оффсет только после работы.
/// Конвейер работает ровно в своём прогоне - контейнер поднимается в Init и гасится в Clean.
/// </summary>
internal class DbzKafkaScenarios(
    NpgsqlDataSource dataSource,
    WebClients web,
    IConfiguration configuration,
    IOptions<LoadTestOptions> options,
    ILogger<DbzKafkaScenarios> logger) : IProducerConsumerScenarios
{
    private readonly LoadTestOptions _options = options.Value;
    private IConsumer<Ignore, string>[] _consumers = [];
    private int _partitions;

    public string Name => "dbz-kafka";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
        => HttpProducer.Create($"{Name}-producer", web.Producer, producers, message, messageCount, logger);

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var state = RunState.Current;
        // Подтверждение и сообщение лежат одной парой: разъехаться они не должны даже при ошибке работы
        var pending = new Queue<(ConsumeResult<Ignore, string> Result, Message Message)>[consumers];

        // Одна итерация = одно обработанное сообщение. Копия завершается, когда очередь разобрана:
        // будить её "бюджетом итераций" нельзя - остаток бюджета соседей превратился бы в ложные отказы.
        return Scenario.Create($"{Name}-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            while (pending[index].Count == 0)
            {
                if (!await FillAsync(index, context.ScenarioCancellationToken))
                    return RunState.FinishConsumer(context, state);
            }

            var (consumed, message) = pending[index].Dequeue();

            // Полезная работа: необратимое действие вне транзакции
            using var response = await web.Work.PostAsJsonAsync("/send-email", message, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                state.MarkFailure();
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            state.MarkHandled(true);
            state.MarkAge(DateTime.UtcNow - message.CreatedAt);

            // Оффсет запоминаем только после работы, отправит его клиент сам по таймауту
            _consumers[index].StoreOffset(consumed);
            return Response.Ok();
        })
        .WithInit(_ => PrepareAsync(consumers, pending, state))
        .WithClean(_ => CleanupAsync())
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.KeepConstant(consumers, _options.DrainTimeout));

        // Набираем пачку из топика и одним запросом узнаём, какие сообщения ещё не обработаны.
        // false - очередь разобрана или вышло время, копии больше делать нечего.
        async Task<bool> FillAsync(int index, CancellationToken ct)
        {
            var consumer = _consumers[index];
            var batch = new List<(ConsumeResult<Ignore, string> Result, Message Message)>(_options.BatchSize);

            while (batch.Count < _options.BatchSize)
            {
                // Первое сообщение ждём короткими шагами: иначе копия не заметит, что прогон закончился
                var result = consumer.Consume(batch.Count == 0 ? PollDelay : _options.FlushDelay);
                if (result is null)
                {
                    if (batch.Count > 0) break;
                    if (state.IsDrained || state.IsExpired || ct.IsCancellationRequested) return false;
                    continue;
                }

                var message = DebeziumEnvelope.ReadMessage(result.Message.Value);
                if (message is null)
                {
                    // Не insert: подтверждаем и не тратим на него итерацию
                    consumer.StoreOffset(result);
                    continue;
                }

                batch.Add((result, message));
            }

            if (batch.Count == 0) return true;

            var claimed = await InboxBatch.ClaimAsync(
                dataSource, batch.Select(x => x.Message.Id).ToArray(), $"{Name}-{index}", ct);
            state.MarkDbOp();

            foreach (var (result, message) in batch)
            {
                if (claimed.Contains(message.Id))
                {
                    pending[index].Enqueue((result, message));
                }
                else
                {
                    // Повторная доставка: работа уже сделана, просто подтверждаем
                    state.MarkHandled(false);
                    consumer.StoreOffset(result);
                }
            }

            return true;
        }
    }

    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(100);

    private async Task PrepareAsync(int consumers, Queue<(ConsumeResult<Ignore, string> Result, Message Message)>[] pending, RunState state)
    {
        // Конвейер Debezium тест не поднимает и не гасит: его контейнер держит оператор. Здесь только
        // проверка, что журнал не читает никто лишний - чужой конвейер декодировал бы те же вставки фоном.
        await Slots.WarnOnForeignReadersAsync(dataSource, _options.DebeziumSlotPrefix, _options.KafkaSlot, logger);
        await Reset.RunAsync(dataSource);
        await web.ResetAsync();

        _partitions = consumers * _options.KafkaPartitionsPerConsumer;

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        await RecreateTopicAsync(admin);

        // Группа одна на прогон и своя у каждого прогона: топик пересоздаётся, чужие оффсеты не нужны.
        // Именно группа делит партиции между получателями - по группе на копию каждый забрал бы весь топик.
        var group = $"{Name}-{Guid.NewGuid():N}";

        _consumers = new IConsumer<Ignore, string>[consumers];
        for (var i = 0; i < consumers; i++)
        {
            pending[i] = new Queue<(ConsumeResult<Ignore, string>, Message)>(_options.BatchSize);
            _consumers[i] = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = Bootstrap,
                GroupId = group,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = false,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            }).Build();
            _consumers[i].Subscribe(_options.KafkaTopic);
        }

        await WaitForAssignmentAsync();

        // Отправителей выпускаем только после того, как конвейер действительно стримит: если контейнер
        // Debezium не поднят, прогон честно падает по таймауту, а не меряет пустоту
        await Slots.WaitForStreamingAsync(dataSource, _options.KafkaSlot, _options.ReadyTimeoutSeconds);

        logger.LogInformation("Debezium Server -> Kafka: топик {Topic}, {Partitions} партиций, {Consumers} получателей",
            _options.KafkaTopic, _partitions, consumers);

        state.StartDeadline(_options.DrainTimeout);
        state.ConsumersReady.TrySetResult();
    }

    /// <summary>
    /// Топик пересоздаётся на каждый прогон и всегда до старта Debezium: топик, созданный брокером по первому
    /// сообщению, получил бы одну партицию, и весь свип упёрся бы в одного получателя.
    /// </summary>
    private async Task RecreateTopicAsync(IAdminClient admin)
    {
        try
        {
            await admin.DeleteTopicsAsync([_options.KafkaTopic]);
        }
        catch (DeleteTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.UnknownTopicOrPart))
        {
            // топика и не было
        }

        // Удаление асинхронное: создавать заново можно только когда старый исчез из метаданных
        await WaitForTopicAsync(admin, exists: false);

        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = _options.KafkaTopic,
            NumPartitions = _partitions,
            ReplicationFactor = 1,
        }]);

        await WaitForTopicAsync(admin, exists: true);
    }

    private async Task WaitForTopicAsync(IAdminClient admin, bool exists)
    {
        var deadline = DateTime.UtcNow.AddSeconds(_options.ReadyTimeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            // Метаданные запрашиваем по всему кластеру, а не по имени топика: запрос по имени при
            // включённом автосоздании сам создаёт топик - с одной партицией, чего мы и избегаем
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == _options.KafkaTopic);
            var partitions = topic is null || topic.Error.Code != ErrorCode.NoError ? 0 : topic.Partitions.Count;

            if (exists ? partitions == _partitions : partitions == 0) return;
            await Task.Delay(500);
        }

        throw new TimeoutException($"Топик {_options.KafkaTopic} не пришёл в ожидаемое состояние (exists={exists})");
    }

    /// <summary>
    /// Ждём, пока группа разберёт все партиции и каждому получателю достанется своя доля. Суммы мало:
    /// её удовлетворяет и один получатель, забравший все партиции, - прогон тогда меряет одного вместо N,
    /// а выглядит правдоподобно. Плюс группа должна устояться, иначе ребаланс посреди прогона переотправит
    /// часть сообщений уже другому получателю.
    /// </summary>
    private async Task WaitForAssignmentAsync()
    {
        var expected = _options.KafkaPartitionsPerConsumer;
        var deadline = DateTime.UtcNow.AddSeconds(_options.ReadyTimeoutSeconds);
        var stable = 0;

        while (DateTime.UtcNow < deadline)
        {
            // Consume двигает координацию группы: без него назначение не приедет
            foreach (var consumer in _consumers) consumer.Consume(TimeSpan.FromMilliseconds(100));

            var assigned = _consumers.Select(c => c.Assignment.Count).ToArray();
            if (assigned.Sum() == _partitions && assigned.All(count => count == expected))
            {
                // Три подряд одинаковых снимка: назначение действительно устоялось, а не мелькнуло
                if (++stable >= 3) return;
            }
            else
            {
                stable = 0;
            }

            await Task.Delay(200);
        }

        throw new TimeoutException(
            $"Партиции не разошлись по получателям: {string.Join("/", _consumers.Select(c => c.Assignment.Count))}, ожидалось по {expected}");
    }

    private async Task CleanupAsync()
    {
        foreach (var consumer in _consumers)
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Kafka consumer close failed");
            }

            consumer.Dispose();
        }

        _consumers = [];

        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
            await admin.DeleteTopicsAsync([_options.KafkaTopic]);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Kafka topic {Topic} delete failed", _options.KafkaTopic);
        }

        // Слот Debezium не трогаем: он принадлежит чужому процессу, тест владеет только топиком и таблицами
    }

    private string Bootstrap => configuration.GetConnectionString("kafka")!;
}
