using Microsoft.EntityFrameworkCore;
using NpgsqlTypes;
using ZeroAlloc.Outbox;
using ZeroAlloc.Outbox.EfCore;

[OutboxMessage]
public class Message
{
    public int Id { get; set; }
    public string Content { get; set; } = string.Empty;
    // Инициализатора нет намеренно: время вставки ставит сама база (default now()), поэтому у всех
    // плеч замера одни часы, и это же значение едет в WAL и в конверт Debezium
    public DateTime CreatedAt { get; set; }
}


// Заявка на обработку записи из WAL: кто первым вставил строку с этим LSN, тот её и обрабатывает
public class InboxWal
{
    public NpgsqlLogSequenceNumber Wal { get; set; }
    public string Slot { get; set; } = string.Empty;
}


// Заявка на обработку сообщения из брокера: пишется пачкой, чтобы обработка была идемпотентной
public class Inbox
{
    public int Id { get; set; }
    public string Consumer { get; set; } = string.Empty;
}


public class AppDbContext(DbContextOptions<AppDbContext> opts)
    : DbContext(opts)
{
    public DbSet<Message> Messages { get; set; }
    public DbSet<InboxWal> InboxWal { get; set; }
    public DbSet<Inbox> Inbox { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddOutboxMessages();

        modelBuilder.Entity<Message>(entity =>
        {
            // Время вставки ставит база: одни часы на все плечи замера
            entity.Property(x => x.CreatedAt).HasDefaultValueSql("now()");
        });

        modelBuilder.Entity<InboxWal>(entity =>
        {
            entity.HasKey(x => x.Wal);
            entity.Property(x => x.Slot).HasMaxLength(64);
            // Таблица живёт один прогон и чистится целиком, автовакууму тут делать нечего
            entity.HasStorageParameter("autovacuum_enabled", false);
        });

        modelBuilder.Entity<Inbox>(entity =>
        {
            entity.Property(x => x.Id).ValueGeneratedNever();
            entity.Property(x => x.Consumer).HasMaxLength(64);
            // Таблица живёт один прогон и чистится целиком, автовакууму тут делать нечего
            entity.HasStorageParameter("autovacuum_enabled", false);
        });
    }
}