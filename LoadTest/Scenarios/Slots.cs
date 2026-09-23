using Microsoft.Extensions.Logging;
using Npgsql;

/// <summary>
/// Слоты логической репликации: их держат и Debezium Server, и прямые читатели. Осиротевший слот держит WAL,
/// поэтому каждый прогон начинается и заканчивается зачисткой по префиксу.
/// </summary>
internal static class Slots
{
    public static async Task DropAsync(NpgsqlDataSource db, string prefix, CancellationToken ct = default)
    {
        // Команды с параметрами идут по одной: расширенный протокол не принимает несколько операторов сразу.
        await using var terminate = db.CreateCommand(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE $1 AND active_pid IS NOT NULL");
        terminate.Parameters.Add(P(prefix + "%"));
        await terminate.ExecuteNonQueryAsync(ct);

        await using var cmd = db.CreateCommand(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1");
        cmd.Parameters.Add(P(prefix + "%"));

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 20)
            {
                await Task.Delay(200, ct);   // слот ещё занят только что снятым backend-ом
            }
        }
    }

    /// <summary>
    /// Ждёт, пока CDC-читатель создаст слот и действительно начнёт стримить. Живой контейнер этого ещё не
    /// значит: Quarkus стартует, коннектор проходит фазу снапшота и только потом открывает слот. А всё, что
    /// записано до слота, в поток уже не попадёт - поэтому отправителей выпускаем строго после.
    /// </summary>
    public static async Task WaitForStreamingAsync(NpgsqlDataSource db, string slotName, int timeoutSeconds, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            await using var cmd = db.CreateCommand(
                """
                SELECT count(*) FROM pg_replication_slots s
                WHERE s.slot_name = $1 AND s.active_pid IS NOT NULL AND s.restart_lsn IS NOT NULL
                  AND EXISTS (SELECT 1 FROM pg_stat_replication r WHERE r.pid = s.active_pid AND r.state = 'streaming')
                """);
            cmd.Parameters.Add(P(slotName));
            if (Convert.ToInt64(await cmd.ExecuteScalarAsync(ct)) > 0) return;

            await Task.Delay(500, ct);
        }

        throw new TimeoutException($"Replication slot {slotName} did not start streaming");
    }

    /// <summary>
    /// Активные слоты с таким префиксом. Конвейеры Debezium тест не поднимает и не гасит - их жизненным
    /// циклом управляет оператор, - поэтому перед прогоном он только смотрит, кто ещё читает журнал:
    /// чужой работающий CDC-конвейер декодирует те же вставки и ложится на прогон фоном.
    /// </summary>
    public static async Task<IReadOnlyList<string>> ActiveAsync(NpgsqlDataSource db, string prefix, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1 AND active_pid IS NOT NULL ORDER BY 1");
        cmd.Parameters.Add(P(prefix + "%"));

        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) names.Add(reader.GetString(0));
        return names;
    }

    /// <summary>Предупреждает, если журнал сейчас читает кто-то ещё: цифры прогона будут с чужим фоном.</summary>
    public static async Task WarnOnForeignReadersAsync(NpgsqlDataSource db, string prefix, string expected, ILogger logger,
        CancellationToken ct = default)
    {
        var foreign = (await ActiveAsync(db, prefix, ct)).Where(name => name != expected).ToArray();
        if (foreign.Length == 0) return;

        logger.LogWarning(
            "Журнал одновременно читают чужие конвейеры: {Slots}. Их декодирование ляжет в этот прогон фоном - " +
            "оставьте поднятым только измеряемый конвейер", string.Join(", ", foreign));
    }

    private static NpgsqlParameter P<T>(T value) => new() { Value = value };
}
