using Npgsql;

// Инбокс для брокеров: та же проверка, что и inbox_wal у репликации, только гонки за право
// обработки тут нет — брокер сам отдаёт сообщение одному потребителю
internal static class InboxBatch
{
    public static async Task ClearAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("truncate table inbox");
        await command.ExecuteNonQueryAsync(ct);
    }

    // Пачка — один запрос: отмечаем все id сразу и забираем те, что достались нам.
    // Вторая ветка — на случай, если отметка уже наша: переотправленное сообщение надо обработать
    public static async Task<HashSet<int>> ClaimAsync(
        NpgsqlDataSource dataSource, int[] ids, string consumer, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            """
            with claimed as (
                insert into inbox (id, consumer)
                select unnest($1), $2
                on conflict do nothing
                returning id
            )
            select id from claimed
            union all
            select id from inbox where id = any($1) and consumer = $2
            """);
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = ids });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = consumer });

        var claimed = new HashSet<int>(ids.Length);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            claimed.Add(reader.GetInt32(0));
        }

        return claimed;
    }
}
