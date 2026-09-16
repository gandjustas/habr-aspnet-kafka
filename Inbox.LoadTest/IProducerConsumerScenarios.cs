using NBomber.Contracts;

internal interface IProducerConsumerScenarios
{
    string Name { get; }

    ScenarioProps CreateConsumerScenario(int consumers, int messageCount);
    ScenarioProps CreateProducerScenario(int producers, string message, int messageCount);
}