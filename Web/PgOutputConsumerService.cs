using System.Collections.Concurrent;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

// Штатный клиент: Npgsql.Replication - часть основной библиотеки Npgsql,
// без сторонних зависимостей поверх протокола логической репликации.
public class PgOutputConsumerService(
    IConfiguration configuration,
    [FromKeyedServices("completions-replication")] ConcurrentDictionary<int, TaskCompletionSource<Message>> completions,
    ILogger<PgOutputConsumerService> logger)
    : BackgroundService
{
    public const string PublicationName = "rep_pub";
    public const string SlotName = "rep_slot";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connectionString = configuration.GetConnectionString("database");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var conn = new LogicalReplicationConnection(connectionString);
                await conn.Open(stoppingToken);

                var slot = new PgOutputReplicationSlot(SlotName);
                var options = new PgOutputReplicationOptions(PublicationName, PgOutputProtocolVersion.V4, binary: true);

                await foreach (var message in conn.StartReplication(slot, options, stoppingToken))
                {
                    if (message is InsertMessage insertMessage)
                    {
                        Message msg = await ReadMessageAsync(insertMessage, stoppingToken);
                        completions.Complete(msg.Id, msg);
                    }

                    // Обязательно подтверждаем прочтение, чтобы PG мог очищать WAL
                    conn.SetReplicationStatus(message.WalEnd);
                    await conn.SendStatusUpdate(stoppingToken);
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Replication connection failed, reconnecting");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private static async ValueTask<Message> ReadMessageAsync(InsertMessage insertMessage, CancellationToken ct)
    {
        var columns = insertMessage.Relation.Columns;
        var msg = new Message();

        int i = 0;
        await foreach (var value in insertMessage.NewRow)
        {
            switch (columns[i].ColumnName)
            {
                case "id":
                    msg.Id = await value.Get<int>(ct);
                    break;
                case "content":
                    msg.Content = await value.Get<string>(ct);
                    break;
                case "created_at":
                    msg.CreatedAt = await value.Get<DateTime>(ct);
                    break;
            }
            i++;
        }

        return msg;
    }
}
