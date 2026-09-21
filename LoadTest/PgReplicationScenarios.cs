using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

internal class PgReplicationScenarios(
    NpgsqlDataSource dataSource,
    HttpClient http,
    IServiceProvider services,
    [FromKeyedServices(PgReplicationScenarios.PublicationKey)] string publication,
    [FromKeyedServices(PgReplicationScenarios.SlotKey)] string slot) : IProducerConsumerScenarios
{
    public const string PublicationKey = "pg-publication";
    public const string SlotKey = "pg-slot";

    // Сколько строк набираем перед тем, как одним запросом застолбить их в inbox_wal
    private const int BatchSize = 100;

    // Сколько ждём добора пачки, прежде чем застолбить её неполной: иначе хвост прогона зависнет
    private static readonly TimeSpan FlushDelay = TimeSpan.FromMilliseconds(50);

    public string Name => "Pg logical replication";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        // Аналог produce: отдельной отправки нет, строка попадает в WAL самим фактом вставки
        return Scenario.Create("pg-replication-producer", async context =>
        {
            await using var command = dataSource.CreateCommand(
                "insert into messages (content, created_at) values ($1, $2)");
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = message });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow });
            await command.ExecuteNonQueryAsync(context.ScenarioCancellationToken);
            return Response.Ok();
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(producers, messageCount));
    }

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var stop = new CancellationTokenSource();
        var slots = new string[consumers];
        var connections = new LogicalReplicationConnection[consumers];
        var streams = new IAsyncEnumerator<PgOutputReplicationMessage>[consumers];
        // Незавершённый MoveNextAsync: при отправке неполной пачки его нельзя терять
        var pending = new Task<bool>?[consumers];
        // Застолблённые строки, ждущие своей итерации
        var ready = new Queue<(NpgsqlLogSequenceNumber Wal, Message Message)>[consumers];

        // Слот не раздаётся нескольким читателям, как партиции топика: у каждого свой слот
        // и весь поток целиком. Кто первым застолбил LSN в inbox_wal, тот его и обрабатывает,
        // поэтому одна итерация = одна застолблённая строка, а всего их ровно messageCount.
        return Scenario.Create("pg-replication-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            // Пачка может целиком уйти другим читателям, тогда просто набираем следующую
            while (ready[index].Count == 0)
            {
                if (!await FillAsync(index, stop.Token))
                {
                    return Response.Fail(message: "Поток репликации закончился");
                }
            }

            var (wal, message) = ready[index].Dequeue();

            // Полезная работа: «отправка письма» через веб-приложение
            using var response = await http.PostAsJsonAsync(
                "/send-email", message, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            // Позицию двигаем только после работы, а отправляем её раз на разобранную пачку
            connections[index].SetReplicationStatus(wal);
            if (ready[index].Count == 0)
            {
                await connections[index].SendStatusUpdate(context.ScenarioCancellationToken);
            }

            return Response.Ok();
        })
        .WithInit(async context =>
        {
            // Заявки живут один прогон
            await using (var truncate = dataSource.CreateCommand("truncate table inbox_wal"))
            {
                await truncate.ExecuteNonQueryAsync(stop.Token);
            }

            for (var i = 0; i < consumers; i++)
            {
                slots[i] = $"{slot}_{i}";
                ready[i] = new Queue<(NpgsqlLogSequenceNumber Wal, Message Message)>(BatchSize);
                connections[i] = services.GetRequiredService<LogicalReplicationConnection>();
                await connections[i].Open(stop.Token);

                // Слот живёт ровно один прогон и создаётся до старта продюсера — это аналог свежего топика:
                // будет прочитано всё, что попало в WAL после его создания
                var replicationSlot = await connections[i].CreatePgOutputReplicationSlot(slots[i],
                    slotSnapshotInitMode: LogicalSlotSnapshotInitMode.NoExport,
                    cancellationToken: stop.Token);

                streams[i] = connections[i]
                    .StartReplication(
                        replicationSlot,
                        new PgOutputReplicationOptions(publication, PgOutputProtocolVersion.V4, binary: true),
                        stop.Token)
                    .GetAsyncEnumerator(stop.Token);
            }
        })
        .WithClean(async context =>
        {
            // Токен сценария до Clean не доживает, поэтому зависший MoveNextAsync обрываем своим
            await stop.CancelAsync();
            for (var i = 0; i < consumers; i++)
            {
                try
                {
                    await streams[i].DisposeAsync();
                }
                catch (OperationCanceledException)
                {
                }
                await connections[i].DisposeAsync();
            }
            stop.Dispose();

            // Пока стримящее соединение живо, слот занят им и не удаляется, поэтому сносим их следом
            await using var cleanup = services.GetRequiredService<LogicalReplicationConnection>();
            await cleanup.Open();
            foreach (var name in slots)
            {
                await cleanup.DropReplicationSlot(name);
            }
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(consumers, messageCount));

        // Набираем пачку строк из потока, столбим её одним запросом и одним статусом подтверждаем позицию.
        // false — поток закончился
        async Task<bool> FillAsync(int index, CancellationToken ct)
        {
            var stream = streams[index];
            var batch = new List<(NpgsqlLogSequenceNumber Wal, Message Message)>(BatchSize);
            var alive = true;

            while (batch.Count < BatchSize)
            {
                var move = pending[index] ??= stream.MoveNextAsync().AsTask();

                // Первое сообщение ждём сколько нужно, а добор пачки — только пока есть что добирать
                if (batch.Count > 0 && !move.IsCompleted
                    && await Task.WhenAny(move, Task.Delay(FlushDelay, ct)) != move)
                {
                    break;
                }

                pending[index] = null;
                if (!await move)
                {
                    alive = false;
                    break;
                }

                var walMessage = stream.Current;

                // Кроме самих строк в потоке едут Begin/Relation/Commit и keep-alive
                if (walMessage is InsertMessage insertMessage)
                {
                    batch.Add((walMessage.WalEnd, await ReadMessageAsync(insertMessage, ct)));
                }
            }

            if (batch.Count == 0)
            {
                return alive;
            }

            var claimed = await ClaimAsync(batch.Select(x => x.Wal.ToString()).ToArray(), slots[index], ct);
            for (var i = 0; i < batch.Count; i++)
            {
                if (claimed.Contains(i))
                {
                    ready[index].Enqueue(batch[i]);
                }
            }

            return alive;
        }
    }

    // Пачка — один запрос: вставляем заявки на все LSN сразу и забираем номера тех, что достались нам.
    // Вторая ветка — на случай, если строка уже наша: перечитанный LSN надо обработать, а не пропустить.
    // LSN передаём текстом, потому что массив pg_lsn Npgsql писать не умеет
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

    // Распаковываем строку из WAL, чтобы в замер попала та же работа, что и десериализация в кафке
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
