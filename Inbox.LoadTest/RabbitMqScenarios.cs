using NBomber.Contracts;

internal class RabbitMqScenarios : IProducerConsumerScenarios
{
    public string Name => "RabbitMq";

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        throw new NotImplementedException();
    }

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        throw new NotImplementedException();
    }
}