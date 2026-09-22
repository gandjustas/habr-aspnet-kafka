using System.Diagnostics;
using NBomber.Contracts;
using NBomber.CSharp;

/// <summary>
/// Состояние одного прогона (плечо + число получателей): счётчики и точки времени.
/// Прогоны идут строго по одному, поэтому Program открывает прогон явно, а сценарии читают текущее состояние.
/// </summary>
public class RunState(int messageCount)
{
    private long _sentOk, _sentFail, _processed, _skipped, _failures;
    private long _dbOps;
    private long _firstSendTs, _lastSendTs, _lastProcessedTs;
    // Границы окна замера в тиках UTC: DateTime нельзя читать и писать через Volatile, это структура
    private long _windowStartTicks, _windowEndTicks;

    // Возраст сообщения на момент выполненной работы. Выборка на сообщение, места хватает с запасом.
    private readonly double[] _ageMs = new double[messageCount + 1];
    private int _ageCount;

    public static RunState Current { get; private set; } = new(0);

    /// <summary>Открывает новый прогон: вызывается из Program до создания сценариев.</summary>
    public static RunState StartRun(int messageCount) => Current = new RunState(messageCount);

    public int MessageCount { get; } = messageCount;
    public TaskCompletionSource ConsumersReady { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public long Deadline { get; private set; } = long.MaxValue;

    public long SentOk => Volatile.Read(ref _sentOk);
    public long SentFail => Volatile.Read(ref _sentFail);
    public long Processed => Volatile.Read(ref _processed);
    public long Skipped => Volatile.Read(ref _skipped);
    public long Failures => Volatile.Read(ref _failures);

    /// <summary>
    /// Запросов к базе на стороне получателей. Claim идёт пачкой, поэтому у брокерных плеч это примерно
    /// одно обращение на сотню сообщений, а у веерного чтения - на сотню у каждого из N читателей.
    /// </summary>
    public long DbOps => Volatile.Read(ref _dbOps);

    public bool ProducersDone => SentOk + SentFail >= MessageCount;
    public bool IsDrained => ProducersDone && Processed >= SentOk;
    public bool IsExpired => Stopwatch.GetTimestamp() > Deadline;

    public TimeSpan ProduceTime => Elapsed(Volatile.Read(ref _firstSendTs), Volatile.Read(ref _lastSendTs));
    public TimeSpan DrainTime => Elapsed(Volatile.Read(ref _firstSendTs), Volatile.Read(ref _lastProcessedTs));

    /// <summary>Слив без фазы отправки: добивающая способность получателей после всплеска.</summary>
    public TimeSpan DrainOnlyTime => Elapsed(Volatile.Read(ref _lastSendTs), Volatile.Read(ref _lastProcessedTs));

    /// <summary>
    /// Окно замера ресурсов: от первой отправки до последней выполненной работы. Выборки вне окна
    /// отбрасываются, иначе в средние попадут старт JVM Debezium и создание топика - у брокерных плеч
    /// это минуты, а у прямого чтения ничего, то есть смещение шло бы вдоль сравниваемой оси.
    /// </summary>
    public (DateTime Start, DateTime End) Window => (
        new DateTime(Volatile.Read(ref _windowStartTicks), DateTimeKind.Utc),
        new DateTime(Volatile.Read(ref _windowEndTicks), DateTimeKind.Utc));

    public void StartDeadline(TimeSpan timeout) => Deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

    public void MarkSent()
    {
        if (Interlocked.CompareExchange(ref _firstSendTs, Stopwatch.GetTimestamp(), 0) == 0)
            Volatile.Write(ref _windowStartTicks, DateTime.UtcNow.Ticks);

        Interlocked.Increment(ref _sentOk);
        Volatile.Write(ref _lastSendTs, Stopwatch.GetTimestamp());
    }

    public void MarkSendFailed() => Interlocked.Increment(ref _sentFail);

    /// <summary>Запрос к базе на стороне получателя: один на пачку.</summary>
    public void MarkDbOp() => Interlocked.Increment(ref _dbOps);

    /// <summary>owned = сообщение досталось этому воркеру и работа выполнена; иначе его уже взял другой.</summary>
    public void MarkHandled(bool owned)
    {
        if (owned)
        {
            Interlocked.Increment(ref _processed);
            Volatile.Write(ref _lastProcessedTs, Stopwatch.GetTimestamp());
            Volatile.Write(ref _windowEndTicks, DateTime.UtcNow.Ticks);
        }
        else
        {
            Interlocked.Increment(ref _skipped);
        }
    }

    public void MarkFailure() => Interlocked.Increment(ref _failures);

    /// <summary>
    /// Сколько сообщение прожило от вставки в базу до выполненной работы. Часы PostgreSQL в контейнере и
    /// часы хоста расходятся на сотни миллисекунд, поэтому у самых свежих сообщений разница выходит
    /// отрицательной - её обрезаем: смещение одинаково для всех плеч и заметно только на коротких прогонах.
    /// </summary>
    public void MarkAge(TimeSpan age)
    {
        var index = Interlocked.Increment(ref _ageCount) - 1;
        if (index < _ageMs.Length) _ageMs[index] = Math.Max(0, age.TotalMilliseconds);
    }

    /// <summary>
    /// Перцентили возраста в миллисекундах. Это не задержка конвейера: при методике drain очередь
    /// разбирается с постоянной скоростью, поэтому возраст - линейная рампа, p50 равен половине времени
    /// разбора, а максимум - всему разбору. Число полезно только как проверка формы, не как латентность.
    /// </summary>
    public (double P50, double P95, double Max) Age()
    {
        var taken = _ageMs.Take(Math.Min(Volatile.Read(ref _ageCount), _ageMs.Length)).ToArray();
        if (taken.Length == 0) return (0, 0, 0);

        Array.Sort(taken);
        return (taken[taken.Length * 50 / 100], taken[Math.Min(taken.Length * 95 / 100, taken.Length - 1)], taken[^1]);
    }

    /// <summary>
    /// Ждёт, пока очередь будет отработана: нужен получателям, которые не опрашивают очередь сами
    /// (в RabbitMQ доставку разбирают подписки, а сценарий только ждёт завершения).
    /// </summary>
    public async Task WaitForDrainAsync(CancellationToken ct)
    {
        while (!IsDrained && !IsExpired && !ct.IsCancellationRequested)
            await Task.Delay(50, CancellationToken.None);
    }

    /// <summary>Получатель закончил: очередь отработана или вышло время.</summary>
    public static IResponse FinishConsumer(IScenarioContext context, RunState state)
    {
        if (!state.IsDrained) return Response.Fail(message: "drain timeout");

        context.StopScenario(context.ScenarioInfo.ScenarioName, "очередь отработана");
        return Response.Ok();
    }

    private static TimeSpan Elapsed(long from, long to)
        => from == 0 || to <= from ? TimeSpan.Zero : Stopwatch.GetElapsedTime(from, to);
}
