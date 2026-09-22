using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using NBomber.Contracts;
using NBomber.CSharp;

/// <summary>
/// Отправитель у всех плеч один и тот же: вызов веб-сервиса, который просто пишет строку в базу.
/// Дальше строку разбирают Debezium и прямые читатели WAL - именно это и сравнивается.
/// </summary>
internal static class HttpProducer
{
    public static ScenarioProps Create(string name, HttpClient client, int producers, string content, int messageCount, ILogger logger)
    {
        var state = RunState.Current;
        return Scenario.Create(name, async context =>
        {
            // Ждать готовности получателей приходится в шаге, а не в Init: инициализации NBomber выполняет
            // по порядку регистрации, отправитель идёт первым, и ожидание в его Init стало бы взаимоблокировкой.
            // Цена - завышенная первая итерация у каждой копии, то есть 200 выборок из ста тысяч.
            if (!state.ConsumersReady.Task.IsCompleted) await state.ConsumersReady.Task;

            try
            {
                // Токен сценария не используем: NBomber отменяет его, набрав нужное число итераций,
                // и запросы, что в этот момент в полёте, были бы потеряны.
                using var response = await client.PostAsJsonAsync("/load/messages", new { Content = content }, CancellationToken.None);
                response.EnsureSuccessStatusCode();

                state.MarkSent();
                return Response.Ok();
            }
            catch (Exception ex)
            {
                state.MarkSendFailed();
                logger.LogError(ex, "POST /load/messages failed");
                return Response.Fail(message: ex.Message);
            }
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(producers, messageCount));
    }
}
