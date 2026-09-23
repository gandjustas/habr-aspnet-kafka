using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

/// <summary>
/// Клиенты веб-сервиса: один для отправителей, другой для вызова работы получателями - чтобы пулы соединений
/// не мешали друг другу. Клиенты создаются вручную, мимо ConfigureHttpClientDefaults из ServiceDefaults:
/// он вешает на все HttpClient стандартный обработчик устойчивости, а автоматический ретрай здесь ломает и
/// замер, и корректность - повтор вставки создаст лишнюю строку, повтор работы удвоит счётчик вызовов.
/// </summary>
public class WebClients : IDisposable
{
    public WebClients(IConfiguration configuration, IOptions<LoadTestOptions> options)
    {
        // Адрес кладёт Aspire при WithReference(web).
        var address = new Uri(configuration["services:web:http:0"] ?? "http://localhost:5280");

        Producer = Create(address, options.Value.Producers + 16);
        Work = Create(address, 256);
    }

    public HttpClient Producer { get; }
    public HttpClient Work { get; }

    /// <summary>Обнуляет счётчики работы перед прогоном.</summary>
    public async Task ResetAsync()
    {
        using var response = await Work.PostAsync("/load/reset", content: null);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>Сколько раз работа была вызвана и для скольких разных сообщений.</summary>
    public async Task<(long Calls, long Distinct)> WorkCountsAsync()
    {
        var snapshot = await Work.GetFromJsonAsync<JsonElement>("/load/stats");
        return (snapshot.GetProperty("calls").GetInt64(), snapshot.GetProperty("distinct").GetInt64());
    }

    private static HttpClient Create(Uri address, int maxConnections) =>
        new(new SocketsHttpHandler { MaxConnectionsPerServer = maxConnections, PooledConnectionLifetime = Timeout.InfiniteTimeSpan })
        {
            BaseAddress = address,
            Timeout = TimeSpan.FromSeconds(30),
        };

    public void Dispose()
    {
        Producer.Dispose();
        Work.Dispose();
    }
}
