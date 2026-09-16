using Confluent.Kafka;
using NBomber.Contracts;
using NBomber.CSharp;

internal class KafkaScenarios(IProducer<int, Message> producer, IConsumer<int, Message> consumer) : IProducerConsumerScenarios
{
    private const string Topic = "messages";

    public string Name => "kafka";

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        return Scenario.Create("kafka-producer", async context =>
        {
            await producer.ProduceAsync(Topic, new()
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
        long receivedCounter = 0;
        return Scenario.Create("kafka-consumer", async context =>
        {
            consumer.Subscribe(Topic);
            while (!context.ScenarioCancellationToken.IsCancellationRequested)
            {
                if (Interlocked.Read(ref receivedCounter) >= messageCount) return Response.Ok();
                try
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(10));
                    if (result != null)
                    {
                        Interlocked.Increment(ref receivedCounter);
                    }
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    await Task.Delay(100, context.ScenarioCancellationToken);
                }
            }
            return Response.Ok();
        })
        .WithoutWarmUp()
        .WithLoadSimulations(Simulation.IterationsForConstant(consumers, consumers));
    }
}