using System.Net.Http.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using NBomber.Contracts;
using NBomber.CSharp;
using Npgsql;

internal class KafkaScenarios(
    IProducer<int, Message> producer,
    NpgsqlDataSource dataSource,
    HttpClient http,
    IServiceProvider services,
    [FromKeyedServices(KafkaScenarios.TopicKey)] string topic) : IProducerConsumerScenarios
{
    public const string TopicKey = "kafka-topic";

    // Сколько сообщений набираем перед тем, как одним запросом отметить их в инбоксе
    private const int BatchSize = 100;

    // Сколько ждём добора пачки, прежде чем отметить её неполной: иначе хвост прогона зависнет
    private static readonly TimeSpan FlushDelay = TimeSpan.FromMilliseconds(50);

    public string Name => "kafka";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        return Scenario.Create("kafka-producer", async context =>
        {
            await producer.ProduceAsync(topic, new()
            {
                Key = (int)context.InvocationNumber,
                Value = new()
                {
                    Id = (int)context.InvocationNumber,
                    Content = message,
                    CreatedAt = DateTime.UtcNow
                }
            }, context.ScenarioCancellationToken);
            return Response.Ok();
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(producers, messageCount));
    }

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        var instances = new IConsumer<int, Message>[consumers];
        // Отмеченные в инбоксе сообщения, ждущие своей итерации
        var ready = new Queue<ConsumeResult<int, Message>>[consumers];

        // Одна итерация = одно обработанное сообщение, поэтому итераций ровно столько, сколько сообщений.
        return Scenario.Create("kafka-consumer", async context =>
        {
            var index = context.ScenarioInfo.InstanceNumber;

            // Пачка может целиком оказаться уже обработанной, тогда набираем следующую
            while (ready[index].Count == 0)
            {
                await FillAsync(index, context.ScenarioCancellationToken);
            }

            var result = ready[index].Dequeue();

            // Полезная работа: «отправка письма» через веб-приложение
            using var response = await http.PostAsJsonAsync(
                "/send-email", result.Message.Value, context.ScenarioCancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return Response.Fail(statusCode: ((int)response.StatusCode).ToString());
            }

            // Оффсет запоминаем только после работы, отправит его клиент сам по таймауту
            instances[index].StoreOffset(result);
            return Response.Ok();
        })
        .WithInit(async context =>
        {
            await InboxBatch.ClearAsync(dataSource, CancellationToken.None);

            for (var i = 0; i < consumers; i++)
            {
                ready[i] = new Queue<ConsumeResult<int, Message>>(BatchSize);
                instances[i] = services.GetRequiredService<IConsumer<int, Message>>();
                instances[i].Subscribe(topic);
            }
        })
        .WithClean(context =>
        {
            foreach (var instance in instances)
            {
                instance.Dispose();
            }
            return Task.CompletedTask;
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(consumers, messageCount));

        // Набираем пачку из топика и одним запросом узнаём, какие сообщения ещё не обработаны
        async Task FillAsync(int index, CancellationToken ct)
        {
            var consumer = instances[index];
            var batch = new List<ConsumeResult<int, Message>>(BatchSize);

            while (batch.Count < BatchSize)
            {
                // Первое сообщение ждём сколько нужно, а добор пачки — только пока есть что добирать
                var result = batch.Count == 0 ? consumer.Consume(ct) : consumer.Consume(FlushDelay);
                if (result is null)
                {
                    break;
                }

                batch.Add(result);
            }

            if (batch.Count == 0)
            {
                return;
            }

            var claimed = await InboxBatch.ClaimAsync(
                dataSource, batch.Select(x => x.Message.Value.Id).ToArray(), $"{Name}-{index}", ct);
            foreach (var consumed in batch)
            {
                if (claimed.Contains(consumed.Message.Value.Id))
                {
                    ready[index].Enqueue(consumed);
                }
            }
        }
    }
}
