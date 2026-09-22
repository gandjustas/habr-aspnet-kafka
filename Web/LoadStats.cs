using System.Collections.Concurrent;
using System.Diagnostics;

/// <summary>
/// Счётчики «ничего не делающего» сервиса: сколько раз получатели вызвали работу и для скольких разных
/// сообщений. Совпадение обоих чисел с числом отправленных — независимое от базы доказательство того,
/// что необратимое действие выполнено ровно один раз: инбокс мог бы сойтись сам с собой, а этот сервис
/// про инбокс ничего не знает. Заодно отдаёт нагрузку самого процесса Web: docker stats его не видит,
/// это процесс на хосте.
/// </summary>
public class LoadStats
{
    // Process.GetCurrentProcess() аллоцирует на каждом вызове, а сэмплер ходит сюда раз в секунду
    private static readonly Process Self = Process.GetCurrentProcess();

    private readonly ConcurrentDictionary<int, byte> _ids = new();
    private long _calls;

    public void Work(int id)
    {
        Interlocked.Increment(ref _calls);
        _ids.TryAdd(id, 0);
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _calls, 0);
        _ids.Clear();
    }

    public object Snapshot()
    {
        Self.Refresh();
        return new
        {
            Calls = Interlocked.Read(ref _calls),
            Distinct = _ids.Count,
            CpuMs = Self.TotalProcessorTime.TotalMilliseconds,
            MemoryMb = Self.WorkingSet64 / 1024d / 1024d,
        };
    }
}
