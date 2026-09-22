using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBomber.Contracts.Stats;
using NBomber.CSharp;
using Npgsql;
using Npgsql.Replication;
using Serilog.Events;

// Отправителей 200, получателей до 8, и каждый ждёт ответа веба: пул потоков не должен раскачиваться
ThreadPool.SetMinThreads(512, 512);

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

builder.Services.Configure<LoadTestOptions>(builder.Configuration.GetSection(LoadTestOptions.SectionName));

builder.Services.AddSingleton(sp =>
{
    var connectionString = new NpgsqlConnectionStringBuilder(
        sp.GetRequiredService<IConfiguration>().GetConnectionString("database")) { MaxPoolSize = 32 };
    return NpgsqlDataSource.Create(connectionString.ConnectionString);
});

// Отдельное соединение на каждый слот: репликационный протокол не мультиплексируется
builder.Services.AddTransient(sp =>
    new LogicalReplicationConnection(sp.GetRequiredService<IConfiguration>().GetConnectionString("database")));

builder.AddRabbitMQClient("rmq", configureSettings: settings => settings.DisableTracing = true);

builder.Services.AddSingleton<WebClients>();
builder.Services.AddSingleton<SystemLoadSampler>();

builder.Services.AddSingleton<IProducerConsumerScenarios, DbzKafkaScenarios>();
builder.Services.AddSingleton<IProducerConsumerScenarios, DbzQuorumScenarios>();
// Два режима одного плеча: веерное чтение и оно же с шардированием по advisory-локам
builder.Services.AddSingleton<IProducerConsumerScenarios>(sp =>
    ActivatorUtilities.CreateInstance<PgReplicationScenarios>(sp, false));
builder.Services.AddSingleton<IProducerConsumerScenarios>(sp =>
    ActivatorUtilities.CreateInstance<PgReplicationScenarios>(sp, true));

using var host = builder.Build();

var options = host.Services.GetRequiredService<IOptions<LoadTestOptions>>().Value;
var logger = host.Services.GetRequiredService<ILogger<Program>>();
var db = host.Services.GetRequiredService<NpgsqlDataSource>();
var web = host.Services.GetRequiredService<WebClients>();
var sampler = host.Services.GetRequiredService<SystemLoadSampler>();
var scenarios = host.Services.GetServices<IProducerConsumerScenarios>().ToDictionary(s => s.Name);
var resultsDir = Results.ResolveDirectory(options.ResultsDir);

// Ни один CDC-читатель не должен работать вне своего прогона: фон от чужого конвейера ложится на соседей
// неодинаково. Контейнеры поднимает и гасит сам тест, в Init и Clean своего плеча.
await Docker.StopAsync(options.KafkaResource, logger);
await Docker.StopAsync(options.QuorumResource, logger);
await Slots.DropAsync(db, options.DebeziumSlotPrefix);
await Slots.DropAsync(db, options.WalSlotPrefix);

var plan = options.TransportList
    .Where(name => scenarios.ContainsKey(name))
    .SelectMany(name => options.ConsumerCounts.Select(consumers => (Transport: name, Consumers: consumers)))
    .ToList();

if (plan.Count == 0)
{
    logger.LogError("Нечего гонять: плечи {Transports} не найдены", options.Transports);
    return 1;
}

var results = new List<RunResult>();

// Прогрев в голове свипа: оплачивает JIT всего пути, пулы соединений и метаданные брокеров.
// Без него штраф в 10-20% достался бы тому плечу, которое оказалось первым.
if (options.WarmupMessages > 0)
{
    await RunAsync(plan[0].Transport, 1, options.WarmupMessages, warmup: true);
}

foreach (var (transport, consumers) in plan)
{
    await RunAsync(transport, consumers, options.Messages, warmup: false);
}

// Зонд дрейфа: повторяем первую комбинацию последней. Разошлись больше чем на 10% - значит стенд за время
// свипа изменился, и межплечевые выводы идут с оговоркой.
if (plan.Count > 1)
{
    await RunAsync(plan[0].Transport, plan[0].Consumers, options.Messages, warmup: true, label: "drift");
}

Results.Print(results);
await Results.SaveAsync(results, resultsDir);

// Зонд дрейфа против своего оригинала: если стенд за время свипа изменился, межплечевые выводы
// нужно делать с поправкой, и лучше узнать об этом сразу, а не выводить её потом руками.
var probe = results.LastOrDefault(r => r.Warmup && r.Transport == plan[0].Transport && r.Consumers == plan[0].Consumers);
var origin = results.FirstOrDefault(r => !r.Warmup && r.Transport == plan[0].Transport && r.Consumers == plan[0].Consumers);
if (probe is not null && origin is not null && origin.DrainOnlyPerSecond > 0)
{
    var drift = (probe.DrainOnlyPerSecond - origin.DrainOnlyPerSecond) / origin.DrainOnlyPerSecond * 100;
    logger.LogInformation("Дрейф стенда на {Transport}-c{Consumers}: {Origin}/с в начале против {Probe}/с в конце, {Drift:0.#}%",
        plan[0].Transport, plan[0].Consumers, origin.DrainOnlyPerSecond, probe.DrainOnlyPerSecond, drift);
}

var failed = results.Count(r => !r.Warmup && !r.Success);
if (failed > 0) logger.LogWarning("Прогонов с ошибками: {Failed}", failed);
return failed == 0 ? 0 : 2;

async Task RunAsync(string transport, int consumers, int messages, bool warmup, string? label = null)
{
    var scenario = scenarios[transport];
    var name = $"{transport}-c{consumers}{(warmup ? $"-{label ?? "warmup"}" : "")}";
    var fanOut = transport.StartsWith("wal", StringComparison.Ordinal);

    logger.LogInformation("=== {Name}: {Messages} сообщений, {Producers} отправителей ===",
        name, messages, options.Producers);

    var state = RunState.StartRun(messages);
    sampler.Start();

    try
    {
        var runner = NBomberRunner
            .RegisterScenarios(
                scenario.CreateProducerScenario(options.Producers, options.Content, messages),
                scenario.CreateConsumerScenario(consumers, messages))
            .WithTestSuite("Debezium Server vs прямое чтение WAL")
            .WithTestName(name)
            .WithScenarioCompletionTimeout(options.DrainTimeout + TimeSpan.FromSeconds(30))
            .WithMinimumLogLevel(LogEventLevel.Warning)
            // Живые метрики в консоли стоят процессорного времени, а сам процесс теста - измеряемый сервис
            .DisplayConsoleMetrics(options.ConsoleMetrics);

        runner = options.NBomberReports
            ? runner.WithReportFolder(Path.Combine(resultsDir, "nbomber", name))
                    .WithReportFormats(ReportFormat.Md, ReportFormat.Csv)
            : runner.WithoutReports();

        // NBomber.Run синхронный и блокирует поток целиком
        var stats = await Task.Run(runner.Run);

        var window = state.Window;
        var load = await sampler.StopAsync(window.Start, window.End);

        // Счётчики веба читаем после остановки конвейера: поздняя доставка иначе надует их задним числом
        var counts = await Reset.InboxCountsAsync(db, fanOut);
        var work = await web.WorkCountsAsync();
        var claims = await Reset.WorkerClaimsAsync(db, fanOut);

        var result = RunResult.From(transport, consumers, options, state, stats, counts, work, claims, load, messages, warmup);
        results.Add(result);

        logger.LogInformation("{Name}: {Status}, drain-only {Rate}/с, {CpuMs} мс CPU на сообщение",
            name, result.Success ? "OK" : "FAILED", result.DrainOnlyPerSecond, result.CpuMsPerMessage);
    }
    catch (Exception ex)
    {
        var load = await sampler.StopAsync(DateTime.MinValue, DateTime.MinValue);
        logger.LogError(ex, "{Name} упал", name);
        results.Add(RunResult.Failed(transport, consumers, options, load, ex.Message));

        // Прогон упал в неизвестном состоянии: гасим оба конвейера и сносим все слоты
        await Docker.StopAsync(options.KafkaResource, logger);
        await Docker.StopAsync(options.QuorumResource, logger);
        await Slots.DropAsync(db, options.DebeziumSlotPrefix);
        await Slots.DropAsync(db, options.WalSlotPrefix);
    }

    await Results.SaveProgressAsync(results, resultsDir);

    // Чекпоинт сейчас, чтобы он не случился посреди следующего прогона и не достался соседу
    await Reset.CheckpointAsync(db);
    await Task.Delay(TimeSpan.FromSeconds(options.CooldownSeconds));
}
