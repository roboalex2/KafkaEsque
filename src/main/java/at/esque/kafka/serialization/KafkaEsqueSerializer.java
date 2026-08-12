package at.esque.kafka.serialization;

import at.esque.kafka.JsonUtils;
import at.esque.kafka.MessageType;
import at.esque.kafka.cluster.TopicMessageTypeConfig;
import at.esque.kafka.handlers.ConfigHandler;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class KafkaEsqueSerializer implements Serializer<Object> {

    private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
    private final KafkaProtobufSerializer<Message> protobufSerializer = new KafkaProtobufSerializer<>();

    private static final List<MessageType> AVRO_TYPES = Arrays.asList(MessageType.AVRO, MessageType.AVRO_TOPIC_RECORD_NAME_STRATEGY);
    private static final List<MessageType> REQUIRES_SCHEMAREGISTRY_TYPES = Arrays.asList(MessageType.AVRO, MessageType.AVRO_TOPIC_RECORD_NAME_STRATEGY, MessageType.PROTOBUF_SR);

    private final Map<MessageType, SerializerWrapper<?>> serializerMap = new EnumMap<>(MessageType.class);

    public KafkaEsqueSerializer() {
        Arrays.stream(MessageType.values())
            .filter(type -> !REQUIRES_SCHEMAREGISTRY_TYPES.contains(type))
            .forEach(type -> serializerMap.put(type, serializerByType(type)));
    }

    private String clusterId;
    private boolean isKey;
    private ConfigHandler configHandler;

    public Object serialize(byte[] bytes) {
        throw new UnsupportedOperationException("Can't deserialize without topic name");
    }

    public byte[] serialize(String s, Object object) {
        TopicMessageTypeConfig topicConfig = configHandler.getConfigForTopic(clusterId, s);
        if (isKey ? AVRO_TYPES.contains(topicConfig.getKeyType()) : AVRO_TYPES.contains(topicConfig.getValueType())) {
            return avroSerializer.serialize(s, object);
        } else if ((isKey ? MessageType.PROTOBUF_SR.equals(topicConfig.getKeyType()) : MessageType.PROTOBUF_SR.equals(topicConfig.getValueType()))) {
            if (object instanceof Message message)
                return protobufSerializer.serialize(s, message);
            else if (object instanceof String jsonString) {
                return protobufSerializer.serialize(s, JsonUtils.fromJson(jsonString));
            }
            throw new UnsupportedOperationException("Serializer for MessageType " + MessageType.PROTOBUF_SR + " does not support serializing type: " + object.getClass());
        } else {
            SerializerWrapper<?> serializerWrapper = serializerMap.get(isKey ? topicConfig.getKeyType() : topicConfig.getValueType());
            return serializerWrapper.serialize(s, (String) object);
        }
    }


    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        serializerMap.values().forEach(serializer -> serializer.configure(configs, isKey));
        if (configs.get("schema.registry.url") != null) {
            avroSerializer.configure(configs, isKey);
            protobufSerializer.configure(configs, isKey);
        }
        this.clusterId = (String) configs.get("kafkaesque.cluster.id");
        this.configHandler = (ConfigHandler) configs.get("kafkaesque.confighandler");
    }

    public void close() {
    }

    private SerializerWrapper<?> serializerByType(MessageType type) {
        switch (type) {
            case STRING:
                return new SerializerWrapper<String>(s -> s, Serdes.String().serializer());
            case SHORT:
                return new SerializerWrapper<Short>(Short::parseShort, Serdes.Short().serializer());
            case INTEGER:
                return new SerializerWrapper<Integer>(Integer::parseInt, Serdes.Integer().serializer());
            case LONG:
                return new SerializerWrapper<Long>(Long::parseLong, Serdes.Long().serializer());
            case FLOAT:
                return new SerializerWrapper<Float>(Float::parseFloat, Serdes.Float().serializer());
            case DOUBLE:
                return new SerializerWrapper<Double>(Double::parseDouble, Serdes.Double().serializer());
            case BYTEARRAY:
                return new SerializerWrapper<byte[]>(String::getBytes, Serdes.ByteArray().serializer());
            case BYTEBUFFER:
                return new SerializerWrapper<ByteBuffer>(s -> ByteBuffer.wrap(s.getBytes()), Serdes.ByteBuffer().serializer());
            case BYTES:
                return new SerializerWrapper<Bytes>(s -> Bytes.wrap(s.getBytes()), Serdes.Bytes().serializer());
            case BASE64:
                return new SerializerWrapper<String>(s -> s, new Base64Serializer());
            case UUID:
                return new SerializerWrapper<UUID>(UUID::fromString, Serdes.UUID().serializer());
            default:
                throw new UnsupportedOperationException("no serializer for Message type: " + type);
        }
    }

    private static class SerializerWrapper<T> {
        private final Function<String, T> function;
        private final Serializer<T> serializer;

        public SerializerWrapper(Function<String, T> function, Serializer<T> serializer) {
            this.function = function;
            this.serializer = serializer;
        }

        private byte[] serialize(String topic, String value) {
            return serializer.serialize(topic, function.apply(value));
        }

        private void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
        }
    }
}
