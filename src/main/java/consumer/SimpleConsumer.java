package consumer;

import model.AvroDeserializer;
import model.AvroSerializer;
import model.ShareInputAvro;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        Properties config = new Properties();
        try {
            String server = "127.0.0.1:9092";
            config.put("client.id", InetAddress.getLocalHost().getHostName());
            config.put("group.id", "foo");
            config.put("bootstrap.servers", server);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());

            final KafkaConsumer<String, ShareInputAvro> consumer = new KafkaConsumer<String, ShareInputAvro>(config);

            String topicName="com-pk-share-input";
            consumer.subscribe(Collections.singletonList(topicName));
            while (true) {
                ConsumerRecords<String, ShareInputAvro> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, ShareInputAvro> record : records) {
                    System.out.println("Consumer->"+record.value());
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

}
