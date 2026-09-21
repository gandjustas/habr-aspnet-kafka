using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

internal class RabbitMqScenarios(
    IConnection connection,
    NpgsqlDataSource dataSource,
    HttpClient http,
    [FromKeyedServices(RabbitMqScenarios.QueueKey)] string queue) : IProducerConsumerScenarios
{
    public const string QueueKey = "rabbit-queue";

    // Аналог упреждающей выборки кафки: без неё брокер отдаёт следующее сообщение только после подтверждения
    private const ushort Prefetch = 1000;

    // Сколько сообщений набираем перед тем, как одним запросом отметить их в инбоксе
    private const int BatchSize = 100;

    // Сколько ждём добора пачки, прежде чем отметить её неполной: иначе хвост прогона зависнет
    private static readonly TimeSpan FlushDelay = TimeSpan.FromMilliseconds(50);

    public string Name => "RabbitMq";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        IChannel channel = null!;

        return Scenario.Create("rabbit-producer", async context =>
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(new Message
            {
                Id = (int)context.InvocationNumber,
                Content = message,
                CreatedAt = DateTime.UtcNow
            }, JsonSerializerOptions.Web);

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: false,
                basicProperties: new BasicProperties { DeliveryMode = DeliveryModes.Persistent },
                body: body,
                cancellationToken: context.ScenarioCancellationToken);
            return Response.Ok();
        })
        .WithInit(async context =>
        {
            // Подтверждения издателя — аналог Acks=All у кафки: публикация ждёт ответа брокера
            channel = await connection.CreateChannelAsync(new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true));
        })
        .WithClean(async context => await channel.DisposeAsync())
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(producers, messageCount));
    }

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var channels = new IChannel[consumers];
        var buffers = new Channel<(ulong DeliveryTag, byte[] Body)>[consumers];
        // Отмеченные в инбоксе сообщения, ждущие своей итерации
        var ready = new Queue<(ulong DeliveryTag, Message Message)>[consumers];

        // Очередь одна на всех, брокер сам раздаёт сообщения между подписчиками — партиции не нужны.
        // Одна итерация = одно обработанное сообщение, поэтому итераций ровно столько, сколько сообщений.
        return Scenario.Create("rabbit-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            // Пачка может целиком оказаться уже обработанной, тогда набираем следующую
            while (ready[index].Count == 0)
            {
                await FillAsync(index, context.ScenarioCancellationToken);
            }

            var (deliveryTag, message) = ready[index].Dequeue();

            // Полезная работа: «отправка письма» через веб-приложение
            using var response = await http.PostAsJsonAsync(
                "/send-email", message, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            // Подтверждаем только после работы: basic.ack односторонний, ответа брокера всё равно нет
            await channels[index].BasicAckAsync(deliveryTag, multiple: false, context.ScenarioCancellationToken);
            return Response.Ok();
        })
        .WithInit(async context =>
        {
            await InboxBatch.ClearAsync(dataSource, CancellationToken.None);

            for (var i = 0; i < consumers; i++)
            {
                ready[i] = new Queue<(ulong DeliveryTag, Message Message)>(BatchSize);

                var buffer = Channel.CreateUnbounded<(ulong DeliveryTag, byte[] Body)>();
                buffers[i] = buffer;

                channels[i] = await connection.CreateChannelAsync();
                await channels[i].BasicQosAsync(prefetchSize: 0, prefetchCount: Prefetch, global: false);

                // Клиент отдаёт сообщения пушем, а сценарию нужен пулл, поэтому складываем их в буфер.
                // Тело живёт только внутри обработчика, так что копируем его.
                var consumer = new AsyncEventingBasicConsumer(channels[i]);
                consumer.ReceivedAsync += (_, delivery) =>
                {
                    buffer.Writer.TryWrite((delivery.DeliveryTag, delivery.Body.ToArray()));
                    return Task.CompletedTask;
                };
                await channels[i].BasicConsumeAsync(queue, autoAck: false, consumer);
            }
        })
        .WithClean(async context =>
        {
            foreach (var channel in channels)
            {
                await channel.DisposeAsync();
            }
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(consumers, messageCount));

        // Набираем пачку из очереди и одним запросом узнаём, какие сообщения ещё не обработаны
        async Task FillAsync(int index, CancellationToken ct)
        {
            var reader = buffers[index].Reader;
            var batch = new List<(ulong DeliveryTag, Message Message)>(BatchSize);

            while (batch.Count < BatchSize)
            {
                (ulong DeliveryTag, byte[] Body) delivery;

                // Первое сообщение ждём сколько нужно, а добор пачки — только пока есть что добирать
                if (batch.Count == 0)
                {
                    delivery = await reader.ReadAsync(ct);
                }
                else if (!reader.TryRead(out delivery))
                {
                    using var topUp = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    topUp.CancelAfter(FlushDelay);
                    try
                    {
                        delivery = await reader.ReadAsync(topUp.Token);
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        break;
                    }
                }

                batch.Add((delivery.DeliveryTag,
                    JsonSerializer.Deserialize<Message>(delivery.Body, JsonSerializerOptions.Web)!));
            }

            if (batch.Count == 0)
            {
                return;
            }

            var claimed = await InboxBatch.ClaimAsync(
                dataSource, batch.Select(x => x.Message.Id).ToArray(), $"{Name}-{index}", ct);
            foreach (var item in batch)
            {
                if (claimed.Contains(item.Message.Id))
                {
                    ready[index].Enqueue(item);
                }
                else
                {
                    // Уже обработано кем-то раньше — работы нет, подтверждаем сразу
                    await channels[index].BasicAckAsync(item.DeliveryTag, multiple: false, ct);
                }
            }
        }
    }
}
