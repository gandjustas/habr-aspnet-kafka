using System.Net.Http.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

/// <summary>
/// Тот же Debezium Server, но приёмник - quorum-очередь RabbitMQ: конкурирующие получатели, каждое сообщение
/// достаётся одному. Очередь и exchange создаются до старта контейнера: Debezium публикует без mandatory,
/// и непривязанный routing key означал бы тихую потерю всего потока при внешне успешном прогоне.
/// </summary>
internal class DbzQuorumScenarios(
    IServiceProvider services,
    NpgsqlDataSource dataSource,
    WebClients web,
    IOptions<LoadTestOptions> options,
    ILogger<DbzQuorumScenarios> logger) : IProducerConsumerScenarios
{
    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(100);

    private readonly LoadTestOptions _options = options.Value;

    // Соединение Aspire открывается при первом обращении, поэтому берём его внутри прогона
    private IConnection Connection => services.GetRequiredService<IConnection>();
    private IChannel[] _channels = [];

    public string Name => "dbz-quorum";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
        => HttpProducer.Create($"{Name}-producer", web.Producer, producers, message, messageCount, logger);

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var state = RunState.Current;
        var buffers = new Channel<(ulong DeliveryTag, byte[] Body)>[consumers];
        var pending = new Queue<(ulong DeliveryTag, Message Message)>[consumers];

        return Scenario.Create($"{Name}-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            while (pending[index].Count == 0)
            {
                if (!await FillAsync(index, context.ScenarioCancellationToken))
                    return RunState.FinishConsumer(context, state);
            }

            var (deliveryTag, message) = pending[index].Dequeue();

            using var response = await web.Work.PostAsJsonAsync("/send-email", message, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                state.MarkFailure();
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            state.MarkHandled(true);
            state.MarkAge(DateTime.UtcNow - message.CreatedAt);

            // Подтверждаем только после работы: basic.ack односторонний, ответа брокера всё равно нет
            await _channels[index].BasicAckAsync(deliveryTag, multiple: false, context.ScenarioCancellationToken);
            return Response.Ok();
        })
        .WithInit(_ => PrepareAsync(consumers, buffers, pending, state))
        .WithClean(_ => CleanupAsync())
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.KeepConstant(consumers, _options.DrainTimeout));

        // Набираем пачку из буфера подписки и одним запросом узнаём, какие сообщения ещё не обработаны
        async Task<bool> FillAsync(int index, CancellationToken ct)
        {
            var reader = buffers[index].Reader;
            var batch = new List<(ulong DeliveryTag, Message Message)>(_options.BatchSize);

            while (batch.Count < _options.BatchSize)
            {
                if (!reader.TryRead(out var delivery))
                {
                    // Ждём короткими шагами, иначе копия не заметит, что прогон закончился
                    using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    wait.CancelAfter(batch.Count == 0 ? PollDelay : _options.FlushDelay);
                    try
                    {
                        delivery = await reader.ReadAsync(wait.Token);
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        if (batch.Count > 0) break;
                        if (state.IsDrained || state.IsExpired) return false;
                        continue;
                    }
                }

                var message = DebeziumEnvelope.ReadMessage(delivery.Body);
                if (message is null)
                {
                    await _channels[index].BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
                    continue;
                }

                batch.Add((delivery.DeliveryTag, message));
            }

            if (batch.Count == 0) return true;

            var claimed = await InboxBatch.ClaimAsync(
                dataSource, batch.Select(x => x.Message.Id).ToArray(), $"{Name}-{index}", ct);
            state.MarkDbOp();

            foreach (var item in batch)
            {
                if (claimed.Contains(item.Message.Id))
                {
                    pending[index].Enqueue(item);
                }
                else
                {
                    // Уже обработано кем-то раньше - работы нет, подтверждаем сразу
                    state.MarkHandled(false);
                    await _channels[index].BasicAckAsync(item.DeliveryTag, multiple: false, ct);
                }
            }

            return true;
        }
    }

    private async Task PrepareAsync(int consumers, Channel<(ulong, byte[])>[] buffers,
        Queue<(ulong DeliveryTag, Message Message)>[] pending, RunState state)
    {
        await Docker.StopAsync(_options.QuorumResource, logger);
        await Docker.StopAsync(_options.KafkaResource, logger);
        await Slots.DropAsync(dataSource, _options.DebeziumSlotPrefix);
        await Reset.RunAsync(dataSource);
        await web.ResetAsync();

        _channels = new IChannel[consumers];
        for (var i = 0; i < consumers; i++)
        {
            pending[i] = new Queue<(ulong, Message)>(_options.BatchSize);
            buffers[i] = Channel.CreateUnbounded<(ulong, byte[])>();
            var buffer = buffers[i];

            _channels[i] = await Connection.CreateChannelAsync();

            if (i == 0) await DeclareTopologyAsync(_channels[0]);

            await _channels[i].BasicQosAsync(prefetchSize: 0, prefetchCount: _options.RabbitPrefetch, global: false);

            // Клиент отдаёт сообщения пушем, а сценарию нужен пулл, поэтому складываем их в буфер.
            // Тело живёт только внутри обработчика, так что копируем его.
            var consumer = new AsyncEventingBasicConsumer(_channels[i]);
            consumer.ReceivedAsync += (_, delivery) =>
            {
                buffer.Writer.TryWrite((delivery.DeliveryTag, delivery.Body.ToArray()));
                return Task.CompletedTask;
            };
            await _channels[i].BasicConsumeAsync(_options.QuorumQueue, autoAck: false, consumer);
        }

        await Docker.StartAsync(_options.QuorumResource, logger);
        await Slots.WaitForStreamingAsync(dataSource, _options.QuorumSlot, _options.ReadyTimeoutSeconds);

        logger.LogInformation("Debezium Server -> quorum-очередь {Queue}, {Consumers} получателей",
            _options.QuorumQueue, consumers);

        state.StartDeadline(_options.DrainTimeout);
        state.ConsumersReady.TrySetResult();
    }

    /// <summary>Exchange и очередь живут один прогон: хвост предыдущего уходит вместе с ними.</summary>
    private async Task DeclareTopologyAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Direct, durable: true);
        await channel.QueueDeleteAsync(_options.QuorumQueue);

        // Raft-группа с тем же именем сразу после удаления иногда не поднимается с первого раза
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await channel.QueueDeclareAsync(_options.QuorumQueue, durable: true, exclusive: false, autoDelete: false,
                    arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
                break;
            }
            catch (Exception ex) when (attempt < 5)
            {
                logger.LogWarning(ex, "Не удалось объявить quorum-очередь, попытка {Attempt}", attempt + 1);
                await Task.Delay(2000);
            }
        }

        await channel.QueueBindAsync(_options.QuorumQueue, _options.Exchange, _options.QuorumRoutingKey);
    }

    private async Task CleanupAsync()
    {
        await Docker.StopAsync(_options.QuorumResource, logger);

        for (var i = 0; i < _channels.Length; i++)
        {
            try
            {
                if (i == 0) await _channels[i].QueueDeleteAsync(_options.QuorumQueue);
                await _channels[i].DisposeAsync();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "RabbitMQ channel close failed");
            }
        }

        _channels = [];
        await Slots.DropAsync(dataSource, _options.DebeziumSlotPrefix);
    }
}
