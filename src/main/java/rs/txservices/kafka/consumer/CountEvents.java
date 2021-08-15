package rs.txservices.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.TimeZone;

public class CountEvents {

    public static class JsonTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {

            if (record.value() instanceof JsonNode) {
                return ((JsonNode) record.value()).get("ts").longValue();
            }

            throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + record.value());
        }
    }

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tx-events");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<byte[], JsonNode> consumed = Consumed.with(Serdes.ByteArray(), jsonSerde).withTimestampExtractor(new JsonTimestampExtractor());
        final KStream<byte[], JsonNode> events = builder.stream("tx-events-input", consumed);

        KTable<String, JsonNode> counts =
                events
                        .groupBy((key, value) -> value.get("uid").textValue())
                        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5)))
                        .count()
                        .toStream()
                        .map((Windowed<String> key, Long value) -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ssZZZZ");
                            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                            String keyString = String.format("[%s@%s/%s]",
                                    key.key(),
                                    sdf.format(key.window().startTime().getEpochSecond()),
                                    sdf.format(key.window().endTime().getEpochSecond()));
                            final ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
                            valueNode.put("window-start", key.window().start());
                            valueNode.put("count", value);
                            return new KeyValue<>(keyString, (JsonNode) valueNode);
                        })
                        .toTable();


        counts.toStream().to("tx-events-output", Produced.with(Serdes.String(), jsonSerde));
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}