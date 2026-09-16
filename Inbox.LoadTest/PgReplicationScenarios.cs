using NBomber.Contracts;

internal class PgReplicationScenarios : IProducerConsumerScenarios
{
    public string Name => "Pg logical replication";

    public ScenarioProps CreateConsumerScenario(int consumers, int messageCount)
    {
        throw new NotImplementedException();
    }

    public ScenarioProps CreateProducerScenario(int producers, string message, int messageCount)
    {
        throw new NotImplementedException();
    }
}