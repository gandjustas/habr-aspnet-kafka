using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Нагрузка на систему во время прогона: CPU, память и дисковые операции контейнеров плюс процессы веба и
/// самого теста. Каждая выборка помечается временем, а при агрегации остаются только попавшие в окно
/// [первая отправка, последняя выполненная работа]: иначе в средние вошли бы старт JVM Debezium и создание
/// топика - у брокерных плеч это десятки секунд, а у прямого чтения ничего, то есть смещение шло бы ровно
/// вдоль сравниваемой оси.
///
/// Учтите: docker stats --no-stream сам занимает 1-2 с, так что фактический период сэмплирования 2-3 с -
/// средние корректны, а совсем короткие пики могут не попасть в выборку.
/// </summary>
public class SystemLoadSampler(WebClients web, IOptions<LoadTestOptions> options, ILogger<SystemLoadSampler> logger)
{
    private const string LoadTestService = "loadtest";
    private const string WebService = "web";

    // Имена ресурсов Aspire. Контейнер называется "{ресурс}-{суффикс}", поэтому сопоставляем по самому
    // длинному подходящему префиксу: иначе dbz-kafka попал бы в строку kafka и испортил её среднее.
    private static readonly string[] Containers = ["postgres", "kafka", "rmq", "dbz-kafka", "dbz-quorum"];

    private static readonly Process Self = Process.GetCurrentProcess();

    private readonly LoadTestOptions _options = options.Value;
    private readonly Dictionary<string, List<Sample>> _samples = [];
    private CancellationTokenSource? _cts;
    private Task? _loop;
    private TimeSpan _processCpu;
    private long _processTimestamp;
    private double _webCpuMs;
    private long _webTimestamp;

    public void Start()
    {
        _samples.Clear();
        Self.Refresh();
        _processCpu = Self.TotalProcessorTime;
        _processTimestamp = Stopwatch.GetTimestamp();
        _webCpuMs = 0;
        _webTimestamp = 0;
        _cts = new CancellationTokenSource();
        _loop = SampleLoopAsync(_cts.Token);
    }

    /// <summary>Останавливает сбор и агрегирует выборки, попавшие в окно прогона.</summary>
    public async Task<SystemLoad> StopAsync(DateTime windowStart, DateTime windowEnd)
    {
        if (_cts is null || _loop is null) return new SystemLoad([]);

        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
        _cts = null;

        // Окно может не сложиться, если прогон упал до первой отправки: тогда берём всё, что успели снять.
        var window = windowEnd > windowStart;
        var windowSeconds = window ? (windowEnd - windowStart).TotalSeconds : 0;

        var services = Containers.Append(WebService).Append(LoadTestService)
            .Select(service => (Service: service, Taken: Filter(service, windowStart, windowEnd, window)))
            .Where(x => x.Taken.Count > 0)
            .Select(x =>
            {
                var cpuAvg = x.Taken.Average(s => s.CpuPercent);
                return new ServiceLoad(
                    x.Service,
                    CpuPercentAvg: Math.Round(cpuAvg, 1),
                    CpuPercentMax: Math.Round(x.Taken.Max(s => s.CpuPercent), 1),
                    // Процессорные секунды за окно: проценты сравнимы только между прогонами равной длины,
                    // а прогоны здесь разной. Это и есть число, по которому считается цена сообщения.
                    CpuSeconds: Math.Round(cpuAvg / 100 * windowSeconds, 1),
                    MemoryMbAvg: Math.Round(x.Taken.Average(s => s.MemoryMb)),
                    MemoryMbMax: Math.Round(x.Taken.Max(s => s.MemoryMb)),
                    // Дисковые счётчики докера накопительные, поэтому за окно берём разницу. При единственной
                    // выборке разницы нет - такую цифру не показываем, вместо неё null.
                    DiskReadMb: Delta(x.Taken, s => s.DiskReadMb),
                    DiskWriteMb: Delta(x.Taken, s => s.DiskWriteMb));
            })
            .ToArray();

        return new SystemLoad(services, Math.Round(windowSeconds, 1));
    }

    private List<Sample> Filter(string service, DateTime start, DateTime end, bool window)
    {
        if (!_samples.TryGetValue(service, out var taken)) return [];
        if (!window) return taken;

        var inside = taken.Where(s => s.At >= start && s.At <= end).ToList();
        // Прогон короче периода сэмплирования: лучше показать то, что есть, чем потерять строку целиком.
        return inside.Count > 0 ? inside : taken;
    }

    private static double? Delta(List<Sample> taken, Func<Sample, double> selector)
    {
        if (taken.Count < 2) return null;
        return Math.Round(taken.Max(selector) - taken.Min(selector), 1);
    }

    private async Task SampleLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            SampleProcess();
            await SampleWebAsync(ct);
            await SampleContainersAsync(ct);

            try
            {
                await Task.Delay(_options.SampleIntervalMs, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>CPU процесса теста за тик, в процентах одного ядра: 250% - два с половиной ядра.</summary>
    private void SampleProcess()
    {
        Self.Refresh();
        var cpu = Self.TotalProcessorTime;
        var elapsed = Stopwatch.GetElapsedTime(_processTimestamp).TotalSeconds;
        var cpuPercent = elapsed > 0 ? (cpu - _processCpu).TotalSeconds / elapsed * 100 : 0;

        _processCpu = cpu;
        _processTimestamp = Stopwatch.GetTimestamp();

        Add(LoadTestService, new Sample(DateTime.UtcNow, cpuPercent, Self.WorkingSet64 / 1024d / 1024d, 0, 0));
    }

    /// <summary>
    /// Web - процесс на хосте, docker stats его не видит, поэтому он сам отдаёт свои CPU и память
    /// тем же эндпоинтом, которым отчитывается о выполненной работе.
    /// </summary>
    private async Task SampleWebAsync(CancellationToken ct)
    {
        try
        {
            var (cpuMs, memoryMb) = await web.ProcessLoadAsync(ct);

            var elapsed = _webTimestamp == 0 ? 0 : Stopwatch.GetElapsedTime(_webTimestamp).TotalMilliseconds;
            var cpuPercent = elapsed > 0 ? (cpuMs - _webCpuMs) / elapsed * 100 : 0;

            _webCpuMs = cpuMs;
            _webTimestamp = Stopwatch.GetTimestamp();

            if (elapsed > 0) Add(WebService, new Sample(DateTime.UtcNow, cpuPercent, memoryMb, 0, 0));
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "web stats unavailable");
        }
    }

    /// <summary>Снимок CPU, памяти и дискового ввода-вывода контейнеров; молча пропускается, если docker недоступен.</summary>
    private async Task SampleContainersAsync(CancellationToken ct)
    {
        try
        {
            using var docker = Process.Start(new ProcessStartInfo("docker",
                "stats --no-stream --format {{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.BlockIO}}")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            })!;

            var output = await docker.StandardOutput.ReadToEndAsync(ct);
            await docker.WaitForExitAsync(ct);

            var at = DateTime.UtcNow;
            foreach (var line in output.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = line.Split('|');
                if (parts.Length != 4) continue;

                var service = Containers
                    .Where(name => parts[0].StartsWith(name, StringComparison.OrdinalIgnoreCase))
                    .MaxBy(name => name.Length);
                if (service is null) continue;

                var blockIo = parts[3].Split('/');
                Add(service, new Sample(
                    At: at,
                    CpuPercent: ParsePercent(parts[1]),
                    MemoryMb: ParseSizeMb(parts[2].Split('/')[0]),
                    DiskReadMb: ParseSizeMb(blockIo.ElementAtOrDefault(0)),
                    DiskWriteMb: ParseSizeMb(blockIo.ElementAtOrDefault(1))));
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "docker stats unavailable");
        }
    }

    private void Add(string service, Sample sample)
    {
        if (!_samples.TryGetValue(service, out var taken)) _samples[service] = taken = [];
        taken.Add(sample);
    }

    private static double ParsePercent(string value) =>
        double.TryParse(value.Trim().TrimEnd('%'), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) ? parsed : 0;

    /// <summary>Разбирает размеры docker stats ("123.4MiB", "1.2GB") в мегабайты.</summary>
    private static double ParseSizeMb(string? value)
    {
        value = value?.Trim();
        if (string.IsNullOrEmpty(value)) return 0;

        var digits = 0;
        while (digits < value.Length && (char.IsAsciiDigit(value[digits]) || value[digits] == '.')) digits++;

        if (!double.TryParse(value[..digits], NumberStyles.Float, CultureInfo.InvariantCulture, out var amount)) return 0;

        var unit = value[digits..].Trim().ToLowerInvariant();
        var bytes = unit switch
        {
            "b" => amount,
            "kb" => amount * 1000,
            "kib" => amount * 1024,
            "mb" => amount * 1000 * 1000,
            "mib" => amount * 1024 * 1024,
            "gb" => amount * 1000d * 1000 * 1000,
            "gib" => amount * 1024d * 1024 * 1024,
            "tb" => amount * 1000d * 1000 * 1000 * 1000,
            "tib" => amount * 1024d * 1024 * 1024 * 1024,
            _ => amount,
        };

        return bytes / 1024 / 1024;
    }

    private readonly record struct Sample(DateTime At, double CpuPercent, double MemoryMb, double DiskReadMb, double DiskWriteMb);
}

/// <summary>Нагрузка одного сервиса за окно прогона.</summary>
public record ServiceLoad(
    string Service,
    double CpuPercentAvg,
    double CpuPercentMax,
    double CpuSeconds,
    double MemoryMbAvg,
    double MemoryMbMax,
    double? DiskReadMb,
    double? DiskWriteMb);

public record SystemLoad(IReadOnlyList<ServiceLoad> Services, double WindowSeconds = 0);
