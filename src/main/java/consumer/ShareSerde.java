package consumer;

import model.AvroDeserializer;
import model.AvroSerializer;
import model.ShareInputAvro;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class ShareSerde extends Serdes {
    public static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    static final class ShareInputAvroSerde extends WrapperSerde<ShareInputAvro> {
        ShareInputAvroSerde() {
            super(new AvroSerializer<>(), new AvroDeserializer<>());
        }
    }

    static public Serde<ShareInputAvro> ShareInputSerde() {
        ShareInputAvroSerde serde =  new ShareInputAvroSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(VALUE_CLASS_NAME_CONFIG, ShareInputAvro.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
