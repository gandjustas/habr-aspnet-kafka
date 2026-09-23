using System.Net.Http.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

/// <summary>
/// Прямое чтение логической репликации без брокера: у каждого получателя свой слот на общую публикацию,
/// поэтому все видят каждую строку, а право на обработку разыгрывается в inbox_wal.
///
/// Режим sharded снимает веерный налог: строки делятся на шарды по id, владение шардом - сессионный
/// advisory-лок, и в базу получатель ходит только за своими строками. Ограничение этого порта: досылки
/// незавершённой работы при подборе чужого шарда нет (в пачечном инбоксе нет отметки done), поэтому схема
/// рассчитана на передачу шарда между сообщениями, а не на смерть воркера посреди работы.
/// </summary>
internal class PgReplicationScenarios(
    bool sharded,
    NpgsqlDataSource dataSource,
    WebClients web,
    IServiceProvider services,
    IConfiguration configuration,
    IOptions<LoadTestOptions> options,
    ILogger<PgReplicationScenarios> logger) : IProducerConsumerScenarios
{
    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(100);

    private readonly LoadTestOptions _options = options.Value;

    public string Name => sharded ? "wal-sharded" : "wal";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
        => HttpProducer.Create($"{Name}-producer", web.Producer, producers, message, messageCount, logger);

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var state = RunState.Current;
        var stop = new CancellationTokenSource();
        var slots = new string[consumers];
        var connections = new LogicalReplicationConnection[consumers];
        var streams = new IAsyncEnumerator<PgOutputReplicationMessage>[consumers];
        var shards = new ShardOwnership?[consumers];
        // Незавершённый MoveNextAsync: при отправке неполной пачки его нельзя терять
        var moving = new Task<bool>?[consumers];
        // Закреплённые строки, ждущие своей итерации
        var pending = new Queue<(NpgsqlLogSequenceNumber Wal, Message Message)>[consumers];

        return Scenario.Create($"{Name}-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            while (pending[index].Count == 0)
            {
                if (!await FillAsync(index, context.ScenarioCancellationToken))
                    return RunState.FinishConsumer(context, state);
            }

            var (wal, message) = pending[index].Dequeue();

            using var response = await web.Work.PostAsJsonAsync("/send-email", message, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                state.MarkFailure();
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            state.MarkHandled(true);
            state.MarkAge(DateTime.UtcNow - message.CreatedAt);

            // Позицию двигаем только после работы, а отправляем её раз на разобранную пачку
            connections[index].SetReplicationStatus(wal);
            if (pending[index].Count == 0)
            {
                await connections[index].SendStatusUpdate(context.ScenarioCancellationToken);
            }

            return Response.Ok();
        })
        .WithInit(_ => PrepareAsync(consumers, slots, connections, streams, shards, moving, pending, stop, state))
        .WithClean(_ => CleanupAsync(consumers, connections, streams, shards, moving, stop))
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.KeepConstant(consumers, _options.DrainTimeout));

        // Набираем пачку строк из потока и одним запросом закрепляем её за собой.
        // false - прогон закончился, копии больше делать нечего.
        async Task<bool> FillAsync(int index, CancellationToken ct)
        {
            var stream = streams[index];
            var ownership = shards[index];
            var batch = new List<(NpgsqlLogSequenceNumber Wal, Message Message)>(_options.BatchSize);

            // Владение пересматриваем между сообщениями, в том же потоке: тогда при отдаче шарда
            // незавершённой работы по нему у нас заведомо нет
            if (ownership is not null) await ownership.RefreshIfDueAsync(ct);

            while (batch.Count < _options.BatchSize)
            {
                var move = moving[index] ??= stream.MoveNextAsync().AsTask();

                // Ждём короткими шагами, иначе копия не заметит, что прогон закончился
                var wait = batch.Count == 0 ? PollDelay : _options.FlushDelay;
                if (!move.IsCompleted && await Task.WhenAny(move, Task.Delay(wait, ct)) != move)
                {
                    if (batch.Count > 0) break;
                    if (state.IsDrained || state.IsExpired || ct.IsCancellationRequested) return false;
                    continue;
                }

                moving[index] = null;
                if (!await move) return false;

                var walMessage = stream.Current;

                // Кроме самих строк в потоке едут Begin/Relation/Commit и keep-alive
                if (walMessage is not InsertMessage insertMessage)
                {
                    // Позицию служебных сообщений всё равно подтверждаем, иначе слот не двигается
                    connections[index].SetReplicationStatus(walMessage.WalEnd);
                    continue;
                }

                var message = await ReadMessageAsync(insertMessage, ct);

                // Чужой шард пропускаем, не обращаясь к базе. Ничей берём: claim разрулит, если возьмут двое
                if (ownership is not null && !ownership.MayClaim(message.Id))
                {
                    connections[index].SetReplicationStatus(walMessage.WalEnd);
                    continue;
                }

                batch.Add((walMessage.WalEnd, message));
            }

            if (batch.Count == 0) return true;

            var claimed = await ClaimAsync(batch.Select(x => x.Wal.ToString()).ToArray(), slots[index], ct);
            state.MarkDbOp();

            for (var i = 0; i < batch.Count; i++)
            {
                if (claimed.Contains(i))
                {
                    pending[index].Enqueue(batch[i]);
                }
                else
                {
                    // Строку уже забрал сосед: работы нет, только двигаем свою позицию
                    state.MarkHandled(false);
                    connections[index].SetReplicationStatus(batch[i].Wal);
                }
            }

            return true;
        }
    }

    private async Task PrepareAsync(int consumers, string[] slots, LogicalReplicationConnection[] connections,
        IAsyncEnumerator<PgOutputReplicationMessage>[] streams, ShardOwnership?[] shards, Task<bool>?[] moving,
        Queue<(NpgsqlLogSequenceNumber Wal, Message Message)>[] pending, CancellationTokenSource stop, RunState state)
    {
        // Конвейеры Debezium в этом прогоне не участвуют: если их контейнеры подняты, они декодируют
        // те же вставки рядом, и это видно в предупреждении. Гасит их оператор, не тест.
        await Slots.WarnOnForeignReadersAsync(dataSource, _options.DebeziumSlotPrefix, expected: "", logger);
        await Slots.DropAsync(dataSource, _options.WalSlotPrefix);
        await Reset.RunAsync(dataSource);
        await web.ResetAsync();

        for (var i = 0; i < consumers; i++)
        {
            slots[i] = $"{_options.WalSlotPrefix}_{i}";
            pending[i] = new Queue<(NpgsqlLogSequenceNumber, Message)>(_options.BatchSize);
            connections[i] = services.GetRequiredService<LogicalReplicationConnection>();
            await connections[i].Open(stop.Token);

            // Слот живёт ровно один прогон и создаётся до старта отправителей: будет прочитано всё,
            // что попало в WAL после его создания, и ничего из записанного раньше
            var replicationSlot = await connections[i].CreatePgOutputReplicationSlot(slots[i],
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.NoExport,
                cancellationToken: stop.Token);

            streams[i] = connections[i]
                .StartReplication(
                    replicationSlot,
                    new PgOutputReplicationOptions(_options.Publication, PgOutputProtocolVersion.V4, binary: true),
                    stop.Token)
                .GetAsyncEnumerator(stop.Token);

            // Перечислитель ленивый: пока не запрошено первое сообщение, репликация не стартует и слот
            // остаётся неактивным. Запрос оставляем висеть - его подхватит первый же FillAsync.
            moving[i] = streams[i].MoveNextAsync().AsTask();

            if (sharded)
            {
                shards[i] = new ShardOwnership(configuration.GetConnectionString("database")!,
                    _options.ShardCount, _options.ShardRefresh, logger);
                await shards[i]!.JoinAsync(consumers, stop.Token);
            }
        }

        // Разбор владения до первой строки: иначе первый получатель считает себя единственным
        foreach (var ownership in shards) if (ownership is not null) await ownership.RefreshIfDueAsync(stop.Token, force: true);

        foreach (var slot in slots)
            await Slots.WaitForStreamingAsync(dataSource, slot, _options.ReadyTimeoutSeconds);

        logger.LogInformation("Прямое чтение WAL: {Consumers} слотов, шардирование {Sharded}", consumers, sharded);

        state.StartDeadline(_options.DrainTimeout);
        state.ConsumersReady.TrySetResult();
    }

    private async Task CleanupAsync(int consumers, LogicalReplicationConnection[] connections,
        IAsyncEnumerator<PgOutputReplicationMessage>[] streams, ShardOwnership?[] shards, Task<bool>?[] moving,
        CancellationTokenSource stop)
    {
        // Токен сценария до Clean не доживает, поэтому зависший MoveNextAsync обрываем своим
        await stop.CancelAsync();

        // Каждый шаг в своём try: оборванный поток репликации кидает не только OperationCanceledException
        // (Npgsql отвечает на Dispose ещё и NotSupportedException), а одно такое исключение уносило с собой
        // освобождение advisory-локов - следующий прогон не находил свободного номера участника.
        for (var i = 0; i < consumers; i++)
        {
            // Незавершённое чтение надо дождаться до закрытия соединения: иначе асинхронный перечислитель
            // Npgsql пытается завершить уже завершённую задачу и роняет процесс из пула потоков
            if (moving[i] is not null)
            {
                await SafeAsync(() => new ValueTask(moving[i]!), "await pending replication read");
                moving[i] = null;
            }

            await SafeAsync(() => streams[i].DisposeAsync(), "dispose replication stream");
            await SafeAsync(() => connections[i].DisposeAsync(), "dispose replication connection");
            if (shards[i] is not null) await SafeAsync(() => shards[i]!.DisposeAsync(), "release shard locks");
        }

        stop.Dispose();

        // Пока стримящее соединение живо, слот занят им и не удаляется, поэтому сносим их следом
        await Slots.DropAsync(dataSource, _options.WalSlotPrefix);
    }

    private async Task SafeAsync(Func<ValueTask> action, string what)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cleanup: {What} failed", what);
        }
    }

    /// <summary>
    /// Пачка - один запрос: вставляем заявки на все LSN сразу и забираем номера тех, что достались нам.
    /// Вторая ветка - на случай, если строка уже наша: перечитанный LSN надо обработать, а не пропустить.
    /// LSN передаём текстом, потому что массив pg_lsn Npgsql писать не умеет.
    /// </summary>
    private async Task<HashSet<int>> ClaimAsync(string[] wals, string slot, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            """
            with input as (
                select ord, txt::pg_lsn as wal
                from unnest($1::text[]) with ordinality as t(txt, ord)
            ),
            claimed as (
                insert into inbox_wal (wal, slot)
                select wal, $2 from input
                on conflict do nothing
                returning wal
            )
            select i.ord from input i join claimed c on c.wal = i.wal
            union all
            select i.ord from input i join inbox_wal w on w.wal = i.wal and w.slot = $2
            """);
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = wals });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = slot });

        var claimed = new HashSet<int>(wals.Length);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            // ordinality нумерует с единицы
            claimed.Add((int)reader.GetInt64(0) - 1);
        }

        return claimed;
    }

    /// <summary>Распаковываем строку из WAL: в замер должна попасть та же работа, что и разбор конверта у брокеров.</summary>
    private static async ValueTask<Message> ReadMessageAsync(InsertMessage insertMessage, CancellationToken ct)
    {
        var columns = insertMessage.Relation.Columns;
        var message = new Message();

        var i = 0;
        await foreach (var value in insertMessage.NewRow)
        {
            switch (columns[i].ColumnName)
            {
                case "id":
                    message.Id = await value.Get<int>(ct);
                    break;
                case "content":
                    message.Content = await value.Get<string>(ct);
                    break;
                case "created_at":
                    message.CreatedAt = await value.Get<DateTime>(ct);
                    break;
            }
            i++;
        }

        return message;
    }
}
