using System.Collections.Concurrent;

public static class CompletionsExtensions
{
    // Consumer side: the event may arrive before the HTTP handler starts waiting for it,
    // so we can't just GetOrAdd+TrySetResult+TryRemove - that drops the result on the floor
    // when nobody is waiting yet. Instead, leave a completed TCS behind for the handler to pick up.
    public static void Complete<TKey, TValue>(this ConcurrentDictionary<TKey, TaskCompletionSource<TValue>> completions, TKey key, TValue value)
        where TKey : notnull
    {
        completions.AddOrUpdate(key,
            addValueFactory: _ =>
            {
                var tcs = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
                tcs.SetResult(value);
                return tcs;
            },
            updateValueFactory: (_, existing) =>
            {
                existing.TrySetResult(value);
                return existing;
            });
    }

    // Producer side: register (or pick up an already-completed) waiter, then clean it up ourselves.
    public static async Task<TValue> WaitForCompletionAsync<TKey, TValue>(
        this ConcurrentDictionary<TKey, TaskCompletionSource<TValue>> completions, TKey key, CancellationToken ct)
        where TKey : notnull
    {
        try
        {
            var tcs = completions.GetOrAdd(key, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
            return await tcs.Task.WaitAsync(ct);
        }
        finally
        {
            completions.TryRemove(key, out _);
        }
    }
}
