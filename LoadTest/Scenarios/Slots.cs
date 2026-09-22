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

    /// <summary>Сколько сейчас слотов с таким префиксом: страж изоляции перед прогоном.</summary>
    public static async Task<long> CountAsync(NpgsqlDataSource db, string prefix, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand("SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE $1");
        cmd.Parameters.Add(P(prefix + "%"));
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
    }

    private static NpgsqlParameter P<T>(T value) => new() { Value = value };
}
