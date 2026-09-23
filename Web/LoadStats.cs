using System.Collections.Concurrent;

/// <summary>
/// Счётчики «ничего не делающего» сервиса: сколько раз получатели вызвали работу и для скольких разных
/// сообщений. Совпадение обоих чисел с числом отправленных — независимое от базы доказательство того,
/// что необратимое действие выполнено ровно один раз: инбокс мог бы сойтись сам с собой, а этот сервис
/// про инбокс ничего не знает. Ресурсы процесса здесь не считаются - их показывает внешний мониторинг.
/// </summary>
public class LoadStats
{
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

    public object Snapshot() => new
    {
        Calls = Interlocked.Read(ref _calls),
        Distinct = _ids.Count,
    };
}
