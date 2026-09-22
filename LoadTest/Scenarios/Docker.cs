using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

/// <summary>
/// Управление контейнерами Debezium Server. Конвейер должен работать только в своём прогоне: иначе тот, кто
/// сейчас не под замером, продолжает декодировать журнал и мешает соседям. Отдельного API у Debezium Server
/// нет - он читает конфигурацию при старте, поэтому прогон открывается стартом контейнера, а закрывается
/// остановкой: позиция хранится в памяти, а слот дропается при выключении, и состояния не остаётся.
/// </summary>
internal static partial class Docker
{
    /// <summary>Имя контейнера, который Aspire создал для ресурса: "{ресурс}-{суффикс}".</summary>
    public static async Task<string?> FindAsync(string resource, CancellationToken ct = default)
    {
        var names = await RunAsync("ps -a --format {{.Names}}", ct);
        var pattern = new Regex($"^{Regex.Escape(resource)}-[a-z0-9]+$", RegexOptions.IgnoreCase);

        return names.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .FirstOrDefault(name => pattern.IsMatch(name));
    }

    public static async Task StartAsync(string resource, ILogger logger, CancellationToken ct = default)
    {
        var container = await FindAsync(resource, ct) ?? throw new InvalidOperationException($"Container for {resource} not found");
        await RunAsync($"start {container}", ct);
        logger.LogInformation("Container {Container} started", container);
    }

    /// <summary>Останавливаем мягко: slot.drop.on.stop срабатывает только при штатном завершении.</summary>
    public static async Task StopAsync(string resource, ILogger logger, CancellationToken ct = default)
    {
        var container = await FindAsync(resource, ct);
        if (container is null) return;

        await RunAsync($"stop -t 30 {container}", ct);
        logger.LogInformation("Container {Container} stopped", container);
    }

    private static async Task<string> RunAsync(string arguments, CancellationToken ct)
    {
        using var docker = Process.Start(new ProcessStartInfo("docker", arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        })!;

        var output = await docker.StandardOutput.ReadToEndAsync(ct);
        var error = await docker.StandardError.ReadToEndAsync(ct);
        await docker.WaitForExitAsync(ct);

        if (docker.ExitCode != 0) throw new InvalidOperationException($"docker {arguments}: {error.Trim()}");
        return output;
    }
}
