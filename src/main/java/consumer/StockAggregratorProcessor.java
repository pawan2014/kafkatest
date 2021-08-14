package consumer;

import model.ShareInputAvro;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class StockAggregratorProcessor extends AbstractProcessor<String, ShareInputAvro> {
    private ProcessorContext context;
    private KeyValueStore<String, ShareInputAvro> kvStore;
    static String STORE = "share-store";

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        kvStore = (KeyValueStore) context.getStateStore(STORE);
    }

    @Override
    public void process(String s, ShareInputAvro shareInputAvro) {
        //System.out.println(shareInputAvro);
        //kvStore.put(shareInputAvro.getShareId().toString(), shareInputAvro);


        if (s.equalsIgnoreCase("EOF")) {
            System.out.println("EOF found");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            handleEOF();
        } else {

            ShareInputAvro oldValue = kvStore.get(shareInputAvro.getTranscationId().toString());
            if (oldValue != null) {
                System.out.println("key found");
                shareInputAvro.setUnits(oldValue.getUnits() + shareInputAvro.getUnits());
                kvStore.put(shareInputAvro.getTranscationId().toString(), shareInputAvro);
            } else {
                System.out.println("put record");
                kvStore.put(shareInputAvro.getTranscationId().toString(), shareInputAvro);
            }

        }

    }

    @Override
    public void close() {
    }

    public void handleEOF() {
        KeyValueIterator<String, ShareInputAvro> iter = kvStore.all();
        while (iter.hasNext()) {
            KeyValue<String, ShareInputAvro> entry = iter.next();
            context.forward(entry.key,entry.value);
            System.out.println("Forwarding for"+entry.key);
        }
        context.commit();

        iter = kvStore.all();
        while (iter.hasNext()) {
            KeyValue<String, ShareInputAvro> entry = iter.next();
            kvStore.delete(entry.key);
        }
        iter.close();


    }

    public void printStore() {
        System.out.println("Loop Store..");
        KeyValueIterator<String, ShareInputAvro> iter = kvStore.all();
        while (iter.hasNext()) {
            KeyValue<String, ShareInputAvro> entry = iter.next();
            System.out.println("  Processed key: " + entry.key + " and value: " + entry.value);
        }
        iter.close();
    }
}
