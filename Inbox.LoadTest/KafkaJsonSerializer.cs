using System.Text.Json;
using Confluent.Kafka;

internal class KafkaJsonSerializer<T>: ISerializer<T>, IDeserializer<T>
{
    public byte[] Serialize(T data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data, JsonSerializerOptions.Web);
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) => isNull ? default : JsonSerializer.Deserialize<T>(data, JsonSerializerOptions.Web)!;
}
