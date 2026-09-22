using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;

/// <summary>
/// Автоматическое шардирование веерного чтения на сессионных advisory-локах PostgreSQL.
///
/// Строки делятся на фиксированное число шардов, и получатель обрабатывает только свои - остальные
/// пропускает, не обращаясь к базе. Владение шардом это факт в базе, а не вычисление по составу группы:
/// у каждого шарда свой лок, и кто его взял, тот и владелец. Лок сессионный, поэтому он снимается сам,
/// как только сессия оборвалась - время жизни сессии и есть лиза, ни heartbeat-таблицы, ни эпох не нужно.
///
/// Правило, без которого схема теряет сообщения: пропускать строку можно, только если её шард **точно**
/// держит кто-то другой. Ничей шард берём, даже если он "не наш" - claim в inbox разрулит, если возьмут двое.
/// Расхождение во мнениях безопасно в сторону лишней работы, но не в сторону пропущенной.
/// </summary>
internal sealed class ShardOwnership : IAsyncDisposable
{
    /// <summary>Пространство ключей advisory-локов: вторая половина ключа - номер шарда.</summary>
    private const int ShardNamespace = 0x7761;

    /// <summary>
    /// Второе пространство - регистрация участника. Нужно потому, что в pg_locks не видно того, кто не
    /// держит ни одного шарда: без отдельного лока первый же получатель считал бы себя единственным
    /// и забирал бы все шарды, а остальным ничего бы не досталось.
    /// </summary>
    private const int MemberNamespace = 0x7762;

    private readonly NpgsqlConnection _connection;
    private readonly int _shardCount;
    private readonly TimeSpan _refreshPeriod;
    private readonly ILogger _logger;

    private readonly HashSet<int> _mine = [];
    private bool[] _isMine;
    private bool[] _heldByOthers;
    private long _refreshedAt;

    public ShardOwnership(string connectionString, int shardCount, TimeSpan refreshPeriod, ILogger logger)
    {
        // Сессионный advisory-лок нельзя держать на пулящемся соединении: оно вернётся в пул вместе с локом,
        // и следующий прогон не найдёт свободного номера участника. Пул выключаем явно - тогда закрытие
        // соединения действительно закрывает сессию, а с ней снимаются все локи.
        _connection = new NpgsqlConnection(
            new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
        _shardCount = shardCount;
        _refreshPeriod = refreshPeriod;
        _logger = logger;
        _isMine = new bool[shardCount];
        _heldByOthers = new bool[shardCount];
    }

    public int OwnedCount => _mine.Count;

    /// <summary>Открывает свою сессию и занимает в ней свободный номер участника.</summary>
    public async Task JoinAsync(int maxMembers, CancellationToken ct)
    {
        await _connection.OpenAsync(ct);

        await using var cmd = new NpgsqlCommand(
            "SELECT min(m) FROM generate_series(0, $1) AS m WHERE pg_try_advisory_lock($2, m)", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = maxMembers - 1 });
        cmd.Parameters.Add(new NpgsqlParameter { Value = MemberNamespace });

        // generate_series остановить нельзя, поэтому лишние занятые номера тут же отпускаем.
        // Сессия предыдущего прогона могла ещё не закрыться на стороне сервера - тогда просто подождём.
        object? index = null;
        for (var attempt = 0; attempt < 10; attempt++)
        {
            index = await cmd.ExecuteScalarAsync(ct);
            if (index is int) break;

            _logger.LogWarning("Свободных номеров участника нет, попытка {Attempt}", attempt + 1);
            await Task.Delay(1000, ct);
        }

        if (index is not int member) throw new InvalidOperationException("No free member slot");

        await using var release = new NpgsqlCommand(
            "SELECT pg_advisory_unlock($1, m) FROM generate_series($2, $3) AS m", _connection);
        release.Parameters.Add(new NpgsqlParameter { Value = MemberNamespace });
        release.Parameters.Add(new NpgsqlParameter { Value = member + 1 });
        release.Parameters.Add(new NpgsqlParameter { Value = maxMembers - 1 });
        await release.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Наш шард или ничей - беремся; чужой пропускаем, не обращаясь к базе.</summary>
    public bool MayClaim(long id)
    {
        var shard = (int)(id % _shardCount);
        return _isMine[shard] || !_heldByOthers[shard];
    }

    /// <summary>
    /// Пересчёт владения. Вызывается из того же потока, что и обработка сообщений, между ними: тогда при
    /// отдаче шарда у нас заведомо нет незавершённой работы по нему, и новый владелец не продублирует её.
    /// Возвращает шарды, которые только что подобрали: по ним надо доделать чужие незавершённые claim.
    /// </summary>
    public async Task<IReadOnlyList<int>> RefreshIfDueAsync(CancellationToken ct, bool force = false)
    {
        if (!force && _refreshedAt != 0 && Stopwatch.GetElapsedTime(_refreshedAt) < _refreshPeriod) return [];
        _refreshedAt = Stopwatch.GetTimestamp();

        var (others, members) = await ReadLocksAsync(ct);

        // Столько шардов нам причитается при текущем составе группы. Себя учитываем всегда.
        var target = (int)Math.Ceiling(_shardCount / (double)Math.Max(1, members));

        if (_mine.Count > target) await ReleaseAsync(_mine.OrderByDescending(s => s).Take(_mine.Count - target).ToArray(), ct);

        var acquired = Array.Empty<int>();
        if (_mine.Count < target)
        {
            var free = Enumerable.Range(0, _shardCount)
                .Where(shard => !_mine.Contains(shard) && !others.Contains(shard))
                .Take(target - _mine.Count)
                .ToArray();

            if (free.Length > 0) acquired = await AcquireAsync(free, ct);
        }

        Publish(others);
        return acquired;
    }

    /// <summary>Кто какие шарды держит и сколько всего участников: pid в pg_locks и есть состав группы.</summary>
    private async Task<(HashSet<int> Others, int Members)> ReadLocksAsync(CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            // objid и classid в pg_locks имеют тип oid, поэтому приводим их явно.
            """
            SELECT objid::bigint, pid, classid::bigint
            FROM pg_locks
            WHERE locktype = 'advisory' AND classid IN ($1::oid, $2::oid) AND objsubid = 2 AND granted
            """,
            _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = ShardNamespace });
        cmd.Parameters.Add(new NpgsqlParameter { Value = MemberNamespace });

        var others = new HashSet<int>();
        var members = 0;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var key = (int)reader.GetInt64(0);

            if (reader.GetInt64(2) == MemberNamespace)
            {
                members++;   // по локу на участника, включая тех, кто пока не держит ни одного шарда
                continue;
            }

            if (!_mine.Contains(key)) others.Add(key);
        }

        return (others, members);
    }

    private async Task<int[]> AcquireAsync(int[] shards, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT s, pg_try_advisory_lock($1, s) FROM unnest($2::int[]) AS s", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = ShardNamespace });
        cmd.Parameters.Add(new NpgsqlParameter { Value = shards });

        var acquired = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            if (reader.GetBoolean(1))
                acquired.Add(reader.GetInt32(0));

        foreach (var shard in acquired) _mine.Add(shard);
        return [.. acquired];
    }

    private async Task ReleaseAsync(int[] shards, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_unlock($1, s) FROM unnest($2::int[]) AS s", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = ShardNamespace });
        cmd.Parameters.Add(new NpgsqlParameter { Value = shards });
        await cmd.ExecuteNonQueryAsync(ct);

        foreach (var shard in shards) _mine.Remove(shard);
        _logger.LogInformation("Released shards {Shards}", string.Join(",", shards));
    }

    private void Publish(HashSet<int> others)
    {
        var mine = new bool[_shardCount];
        var foreign = new bool[_shardCount];

        foreach (var shard in _mine) mine[shard] = true;
        foreach (var shard in others) foreign[shard] = true;

        _isMine = mine;
        _heldByOthers = foreign;
    }

    public async ValueTask DisposeAsync()
    {
        // Локи снимет и закрытие сессии, но снимаем явно: прогон не должен зависеть от того, когда
        // именно сервер заметит обрыв, - следующий стартует через несколько секунд
        try
        {
            await using var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock_all()", _connection);
            await unlock.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "pg_advisory_unlock_all failed");
        }

        await _connection.DisposeAsync();
    }
}
