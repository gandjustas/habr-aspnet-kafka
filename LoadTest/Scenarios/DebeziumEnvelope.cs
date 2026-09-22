using System.Globalization;
using System.Text.Json;

/// <summary>
/// Конверт Debezium без схемы, одинаковый у всех его приёмников: полезная строка лежит в after.
/// SMT ExtractNewRecordState намеренно не подключаем - пусть в замере будет видно, что отдаёт Debezium.
/// </summary>
internal static class DebeziumEnvelope
{
    public static Message? ReadMessage(ReadOnlySpan<byte> body)
    {
        if (body.IsEmpty) return null;

        using var envelope = JsonDocument.Parse(body.ToArray());
        return ReadMessage(envelope);
    }

    public static Message? ReadMessage(string? value)
    {
        if (string.IsNullOrEmpty(value)) return null;

        using var envelope = JsonDocument.Parse(value);
        return ReadMessage(envelope);
    }

    private static Message? ReadMessage(JsonDocument envelope)
    {
        if (!envelope.RootElement.TryGetProperty("after", out var after) || after.ValueKind != JsonValueKind.Object) return null;

        return new Message
        {
            Id = after.GetProperty("id").GetInt32(),
            Content = after.GetProperty("content").GetString() ?? "",
            // timestamptz Debezium отдаёт строкой ISO-8601 в UTC (io.debezium.time.ZonedTimestamp).
            CreatedAt = DateTime.Parse(after.GetProperty("created_at").GetString()!, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal),
        };
    }
}
