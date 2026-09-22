using System.Globalization;
using System.Text;
using System.Text.Json;
using NBomber.Contracts.Stats;

/// <summary>Итог одного прогона: плечо + число получателей.</summary>
public record RunResult(
    string Transport,
    int Consumers,
    int Producers,
    int Messages,
    double ProduceSeconds,
    double SendPerSecond,
    double DrainSeconds,
    double DrainPerSecond,
    double DrainOnlySeconds,
    double DrainOnlyPerSecond,
    double ProducerP95Ms,
    double AgeP50Ms,
    double AgeP95Ms,
    double AgeMaxMs,
    double DbOpsPerMessage,
    long Sent,
    long SendFailed,
    long Processed,
    long Skipped,
    long Failures,
    long InboxRows,
    long DistinctIds,
    long WorkCalls,
    long WorkDistinct,
    long WorkerClaimsMin,
    long WorkerClaimsMax,
    SystemLoad Load,
    bool Warmup = false,
    string? Error = null)
{
    /// <summary>
    /// Работа выполнена ровно один раз для каждого отправленного сообщения. WorkCalls и WorkDistinct приходят
    /// из самого "ничего не делающего" сервиса, то есть проверка не зависит от базы: инбокс мог бы сойтись
    /// сам с собой, а этот сервис про инбокс ничего не знает.
    /// </summary>
    public bool Success => Error is null && SendFailed == 0 && Sent == Messages && Processed == Messages
                           && DistinctIds == Messages
                           && WorkCalls == Messages && WorkDistinct == Messages;

    /// <summary>Процессорные секунды всех сервисов на одно сообщение - главная производная отчёта.</summary>
    public double CpuMsPerMessage => Messages == 0 ? 0
        : Math.Round(Load.Services.Sum(s => s.CpuSeconds) * 1000 / Messages, 3);

    public static RunResult From(string transport, int consumers, LoadTestOptions options, RunState state, NodeStats stats,
        (long Rows, long DistinctIds) counts, (long Calls, long Distinct) work,
        IReadOnlyList<long> workerClaims, SystemLoad load, int messages, bool warmup)
    {
        var produce = state.ProduceTime.TotalSeconds;
        var drain = state.DrainTime.TotalSeconds;
        var drainOnly = state.DrainOnlyTime.TotalSeconds;
        var processed = Math.Max(state.Processed, 1);
        var age = state.Age();

        return new RunResult(
            transport, consumers, options.Producers, messages,
            Math.Round(produce, 2), produce > 0 ? Math.Round(state.SentOk / produce) : 0,
            Math.Round(drain, 2), drain > 0 ? Math.Round(state.Processed / drain) : 0,
            Math.Round(drainOnly, 2), drainOnly > 0 ? Math.Round(state.Processed / drainOnly) : 0,
            Percentile95(stats, $"{transport}-producer"),
            Math.Round(age.P50), Math.Round(age.P95), Math.Round(age.Max),
            Math.Round(state.DbOps / (double)processed, 3),
            state.SentOk, state.SentFail, state.Processed, state.Skipped, state.Failures,
            counts.Rows, counts.DistinctIds, work.Calls, work.Distinct,
            workerClaims.Count > 0 ? workerClaims.Min() : 0, workerClaims.Count > 0 ? workerClaims.Max() : 0,
            load, warmup);
    }

    /// <summary>Прогон не состоялся: строка всё равно попадает в отчёт, чтобы свип не терял историю.</summary>
    public static RunResult Failed(string transport, int consumers, LoadTestOptions options, SystemLoad load, string error) =>
        new(Transport: transport, Consumers: consumers, Producers: options.Producers, Messages: options.Messages,
            ProduceSeconds: 0, SendPerSecond: 0, DrainSeconds: 0, DrainPerSecond: 0, DrainOnlySeconds: 0, DrainOnlyPerSecond: 0,
            ProducerP95Ms: 0, AgeP50Ms: 0, AgeP95Ms: 0, AgeMaxMs: 0, DbOpsPerMessage: 0,
            Sent: 0, SendFailed: 0, Processed: 0, Skipped: 0, Failures: 0,
            InboxRows: 0, DistinctIds: 0, WorkCalls: 0, WorkDistinct: 0,
            WorkerClaimsMin: 0, WorkerClaimsMax: 0, Load: load, Error: error);

    private static double Percentile95(NodeStats stats, string scenario)
    {
        var scenarioStats = stats.ScenarioStats.FirstOrDefault(s => s.ScenarioName == scenario);
        return scenarioStats is null ? 0 : Math.Round(scenarioStats.Ok.Latency.Percent95, 2);
    }
}

public static class Results
{
    private const string Title = "Debezium Server -> Kafka vs RabbitMQ quorum vs прямое чтение WAL";

    private static readonly string[] ThroughputHeader =
    [
        // Возраст сообщения (created_at -> выполненная работа) в таблицу не выносим: при методике drain он
        // равен половине времени разбора по построению, а часы PostgreSQL в контейнере и часы хоста
        // расходятся на сотни миллисекунд. Цифры остаются в json, но выводами по ним быть не может.
        "pipeline", "cons", "produce s", "send/s", "send p95 ms", "drain s", "drain/s", "drain-only s", "drain-only/s",
        "cpu ms/msg", "ops/msg", "processed", "skipped", "work calls", "work uniq",
        "claim min/max", "status",
    ];

    private static readonly string[] LoadHeader =
    [
        "pipeline", "cons", "service", "cpu avg %", "cpu max %", "cpu s", "mem avg MB", "mem max MB",
        "disk read MB", "disk write MB",
    ];

    public static void Print(IReadOnlyList<RunResult> results)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {Title} ===");
        Console.WriteLine(string.Join(" | ", ThroughputHeader));
        foreach (var r in results) Console.WriteLine(string.Join(" | ", ThroughputRow(r)));

        Console.WriteLine();
        Console.WriteLine("=== Нагрузка на сервисы ===");
        Console.WriteLine(string.Join(" | ", LoadHeader));
        foreach (var row in results.SelectMany(LoadRows)) Console.WriteLine(string.Join(" | ", row));
        Console.WriteLine();

        foreach (var failed in results.Where(r => r.Error is not null))
            Console.WriteLine($"{failed.Transport} c{failed.Consumers}: {failed.Error}");
    }

    /// <summary>Промежуточный снимок после каждого прогона: падение свипа не должно терять уже сделанное.</summary>
    public static Task SaveProgressAsync(IReadOnlyList<RunResult> results, string directory) =>
        WriteJsonAsync(results, directory, "results-latest.json");

    public static async Task SaveAsync(IReadOnlyList<RunResult> results, string directory)
    {
        var stamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        await WriteJsonAsync(results, directory, $"results-{stamp}.json");

        // Прогрев в таблицы не идёт: он оплачивает JIT и пулы, и его цифры не про конвейер.
        var measured = results.Where(r => !r.Warmup).ToList();

        var markdown = new StringBuilder()
            .AppendLine($"# {Title}")
            .AppendLine()
            .AppendLine("## Пропускная способность")
            .AppendLine()
            .Append(Table(ThroughputHeader, measured.Select(ThroughputRow)))
            .AppendLine()
            .AppendLine("## Нагрузка на сервисы")
            .AppendLine()
            .Append(Table(LoadHeader, measured.SelectMany(LoadRows)));

        await File.WriteAllTextAsync(Path.Combine(directory, $"results-{stamp}.md"), markdown.ToString());
        Console.WriteLine($"Результаты: {Path.Combine(directory, $"results-{stamp}.md")}");
    }

    private static async Task WriteJsonAsync(IReadOnlyList<RunResult> results, string directory, string fileName)
    {
        Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, fileName),
            JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
    }

    private static string Table(string[] header, IEnumerable<string[]> rows)
    {
        var table = new StringBuilder()
            .AppendLine($"| {string.Join(" | ", header)} |")
            .AppendLine($"|{string.Join("|", header.Select(_ => "---"))}|");
        foreach (var row in rows) table.AppendLine($"| {string.Join(" | ", row)} |");
        return table.ToString();
    }

    private static string[] ThroughputRow(RunResult r) =>
    [
        r.Transport, r.Consumers.ToString(),
        Number(r.ProduceSeconds), Number(r.SendPerSecond), Number(r.ProducerP95Ms),
        Number(r.DrainSeconds), Number(r.DrainPerSecond), Number(r.DrainOnlySeconds), Number(r.DrainOnlyPerSecond),
        Number(r.CpuMsPerMessage), Number(r.DbOpsPerMessage),
        r.Processed.ToString(), r.Skipped.ToString(), r.WorkCalls.ToString(), r.WorkDistinct.ToString(),
        $"{r.WorkerClaimsMin}/{r.WorkerClaimsMax}",
        r.Success ? "OK" : "FAILED",
    ];

    /// <summary>Строки нагрузки: по одной на сервис плюс веб и сам процесс теста.</summary>
    private static IEnumerable<string[]> LoadRows(RunResult r)
    {
        foreach (var service in r.Load.Services)
            yield return
            [
                r.Transport, r.Consumers.ToString(), service.Service,
                Number(service.CpuPercentAvg), Number(service.CpuPercentMax), Number(service.CpuSeconds),
                Number(service.MemoryMbAvg), Number(service.MemoryMbMax),
                Number(service.DiskReadMb), Number(service.DiskWriteMb),
            ];
    }

    private static string Number(double value) => value.ToString("0.##", CultureInfo.InvariantCulture);

    private static string Number(double? value) => value is null ? "n/a" : Number(value.Value);

    /// <summary>Каталог результатов: load-tests/results рядом с solution-файлом.</summary>
    public static string ResolveDirectory(string? configured)
    {
        if (!string.IsNullOrWhiteSpace(configured)) return configured;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "aspnet-kafka.slnx"))) dir = dir.Parent;
        return Path.Combine(dir?.FullName ?? AppContext.BaseDirectory, "load-tests", "results");
    }
}
