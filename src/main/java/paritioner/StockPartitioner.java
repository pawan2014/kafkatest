package paritioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class StockPartitioner implements Partitioner {
    private String speedSensorName;

    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int p = 0;
        if ((keyBytes == null) || (!(key instanceof String)))
            throw new InvalidRecordException("All messages must have Key");
        p = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        System.out.println("Key = " + (String) key + " Partition = " + p);
        return p;
    }

    public void close() {
    }
}
