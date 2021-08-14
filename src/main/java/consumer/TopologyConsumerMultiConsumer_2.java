package consumer;

import model.AvroDeserializer;
import model.AvroSerializer;
import model.ShareInputAvro;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Shows Processor API Topology can be configured from stream API
 */
public class TopologyConsumerMultiConsumer_2 {
    static String STORE = "share-store";

    public static void main(String[] args) {
        Properties config = new Properties();

        KeyValueBytesStoreSupplier storeSupplier =
                Stores.persistentKeyValueStore("my-store");

        StoreBuilder<KeyValueStore<String, ShareInputAvro>> storeBuilder = createStore();

        String server = "127.0.0.1:9092";
        config.put("client.id", "TopologyConsumerMultiConsumer");
        config.put("group.id", "foo");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-processor-api");
        config.put("bootstrap.servers", server);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());

        String topicName = "com-pk-share-input";
        String topicNameOut = "com-pk-share-output";
        StringDeserializer stringDeserializer = new StringDeserializer();
        Deserializer deserializer = new AvroDeserializer();


        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(topicNameOut, Consumed.with(Serdes.String(), ShareSerde.ShareInputSerde()))
                .print(Printed.<String,ShareInputAvro>toSysOut()
                        .withLabel("MY PRONT"));


        // Build Topology
        Topology topology = builder.build(config);


        topology.addSource("Source", stringDeserializer, deserializer, topicName)
                .addProcessor("Process-1", () -> new StockAggregratorProcessor(), "Source")
                .addStateStore(storeBuilder, "Process-1")
                .addSink("SINK-1", topicNameOut, new Serdes.StringSerde().serializer(), new AvroSerializer<ShareInputAvro>(), "Process-1");

        // Start Stream
        KafkaStreams streaming = new KafkaStreams(topology, config);
        streaming.cleanUp();
        streaming.start();

        System.out.println(topology.describe());

        System.out.println("Now started PurchaseProcessor Example");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streaming.close();
            }
        }));

    }

    private static StoreBuilder<KeyValueStore<String, ShareInputAvro>> createStore() {
        StoreBuilder<KeyValueStore<String, ShareInputAvro>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE),
                        Serdes.String(),
                        ShareSerde.ShareInputSerde());
        return storeBuilder;
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Processor-Job");
        props.put("group.id", "test-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-processor-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        // props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
