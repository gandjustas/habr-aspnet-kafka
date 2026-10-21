/// <summary>
/// Водяной знак подтверждения: позицию в потоке можно двигать только до последнего закрытого сообщения,
/// перед которым не осталось незакрытых.
///
/// И оффсет Kafka, и LSN слота - позиционные: подтверждая номер, мы подтверждаем и всё, что перед ним.
/// Поэтому подтверждать чужой claim или служебный конверт сразу нельзя: в пачке перед ним может лежать
/// наше же сообщение, работа по которому ещё не сделана, - и после падения оно не приедет заново.
/// </summary>
internal sealed class AckWatermark<T> where T : IComparable<T>
{
    /// <summary>Взято в работу и ещё не закрыто: подтверждать позиции не ниже самой ранней из них нельзя.</summary>
    private readonly SortedSet<T> _inflight = [];

    /// <summary>Закрытые позиции, которые пока держит кто-то более ранний.</summary>
    private readonly SortedSet<T> _resolved = [];

    private T? _confirmed;

    /// <summary>Сообщение взято в работу.</summary>
    public void Hold(T position) => _inflight.Add(position);

    /// <summary>
    /// Сообщение закрыто: работа сделана, либо её не будет (чужой claim, чужой шард, служебный конверт).
    /// Возвращает true и позицию, до которой теперь можно подтверждать, если она сдвинулась.
    /// </summary>
    public bool TryResolve(T position, out T confirmed)
    {
        _inflight.Remove(position);
        _resolved.Add(position);

        var moved = false;
        while (_resolved.Count > 0 && (_inflight.Count == 0 || _resolved.Min!.CompareTo(_inflight.Min!) < 0))
        {
            _confirmed = _resolved.Min;
            _resolved.Remove(_confirmed!);
            moved = true;
        }

        confirmed = _confirmed!;
        return moved;
    }

    public void Reset()
    {
        _inflight.Clear();
        _resolved.Clear();
        _confirmed = default;
    }
}
