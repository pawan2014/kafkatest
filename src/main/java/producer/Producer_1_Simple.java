package producer;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import model.AvroSerializer;
import model.ShareInputAvro;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer_1_Simple {
    private final Logger mLogger = LoggerFactory.getLogger(Producer_1_Simple.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";

        String topicName="com-pk-share-input";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        props.setProperty("log.flush.interval.messages", "1");
        props.put("linger.ms", 1);



        KafkaProducer<String, ShareInputAvro> mProducer = new KafkaProducer<>(props);

        try {
            List<ShareInputAvro> st = getData("src/main/resources/data/data1.csv");
            st.forEach(data -> {
                System.out.println(">>"+data);
                ProducerRecord<String, ShareInputAvro> record = new ProducerRecord(topicName, data.getTranscationId(), data);
                mProducer.send(record);
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static List<ShareInputAvro> getData(String dataFile) throws IOException {
        File file = new File(dataFile);
        CsvSchema schema = CsvSchema.builder()
                .addColumn("transcationId", CsvSchema.ColumnType.STRING)
                .addColumn("shareId", CsvSchema.ColumnType.STRING)
                .addColumn("type", CsvSchema.ColumnType.STRING)
                .addColumn("cost", CsvSchema.ColumnType.NUMBER)
                .addColumn("units", CsvSchema.ColumnType.NUMBER)
                .build();
        MappingIterator<ShareInputAvro> stockDataIterator = new CsvMapper().readerFor(ShareInputAvro.class).with(schema).readValues(file);
        return stockDataIterator.readAll();
    }
}
