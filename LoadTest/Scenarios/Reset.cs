using Npgsql;

/// <summary>Сброс состояния базы между прогонами и итоговая сверка.</summary>
internal static class Reset
{
    /// <summary>
    /// Чистим заявки и сами сообщения. Идентификаторы намеренно не перезапускаются: сквозная нумерация
    /// между прогонами значит, что хвост предыдущего прогона, если бы он просочился, виден как чужой id,
    /// а не сходится по счётчикам тихо.
    /// </summary>
    public static async Task RunAsync(NpgsqlDataSource db, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand("TRUNCATE inbox; TRUNCATE inbox_wal; TRUNCATE messages;");
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Чекпоинт после прогона: иначе он случится посреди следующего и достанется соседу.</summary>
    public static async Task CheckpointAsync(NpgsqlDataSource db, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand("CHECKPOINT");
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Сверка после прогона: сколько строк закреплено и сколько среди них уникальных сообщений.</summary>
    public static async Task<(long Rows, long DistinctIds)> InboxCountsAsync(
        NpgsqlDataSource db, bool fanOut, CancellationToken ct = default)
    {
        // У веерного чтения заявка лежит по LSN в inbox_wal, у остальных - по id сообщения в inbox.
        var sql = fanOut
            ? "SELECT count(*), count(DISTINCT wal) FROM inbox_wal"
            : "SELECT count(*), count(DISTINCT id) FROM inbox";

        await using var cmd = db.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    /// <summary>Сколько строк закрепил за собой каждый получатель: показывает перекос раздачи.</summary>
    public static async Task<IReadOnlyList<long>> WorkerClaimsAsync(
        NpgsqlDataSource db, bool fanOut, CancellationToken ct = default)
    {
        var sql = fanOut
            ? "SELECT count(*) FROM inbox_wal GROUP BY slot ORDER BY 1"
            : "SELECT count(*) FROM inbox GROUP BY consumer ORDER BY 1";

        await using var cmd = db.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        var claims = new List<long>();
        while (await reader.ReadAsync(ct)) claims.Add(reader.GetInt64(0));
        return claims;
    }
}
