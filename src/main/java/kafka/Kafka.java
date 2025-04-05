package kafka;

import kafka.common.DescribeTopicPartitionsResponse;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataInputStream;
import kafka.record.Record;
import kafka.record.RecordBatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class Kafka {
    private static final String TOPICS_METADATA = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    Map<String, kafka.record.Record.Topic> topics = new HashMap<>();
    Map<UUID, List<Record.Partition>> partitions = new HashMap<>();

    public Kafka() {
        try (final var fileInputStream = new FileInputStream(TOPICS_METADATA)) {
            System.out.println(HexFormat.ofDelimiter("").formatHex(fileInputStream.readAllBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int counter = 0;
        try (FileInputStream fileInputStream = new FileInputStream(TOPICS_METADATA)) {
            DataInput input = new DataInputStream(fileInputStream);
            RecordBatch batch = null;
            while (fileInputStream.available() > 0) {
                try {
                    batch = RecordBatch.deserialize(input);

                    for (Record rec : batch.records()) {
                        if (rec instanceof Record.Topic) {
                            Record.Topic topicRecord = (Record.Topic) rec;
                            topics.put(topicRecord.getTopicName(), topicRecord);
                            System.out.println("loaded topic: " + topicRecord.getTopicName() + " " + topicRecord.getTopicId());
                        } else if (rec instanceof Record.Partition) {
                            Record.Partition partitionRecord = (Record.Partition) rec;
                            partitions.computeIfAbsent(partitionRecord.getTopicID(), key -> new ArrayList<>());
                            List<Record.Partition> partitionList = partitions.get(partitionRecord.getTopicID());
                            partitionList.add(partitionRecord);
                            System.out.println("loaded partition: " + partitionRecord.getTopicID() + " " + partitionRecord.getPartitionID());
                        }
                    }
                } finally {
                    if (batch != null) {
                        counter += 1;
                        System.out.println("finished [" + counter + "] batch= " + batch + ", with available=" + fileInputStream.available());
                    }
                    else {
                        System.out.println("no batch available");
                    }
                }
            }
        } catch (IOException ioEx) {
            System.out.println("error loading ClusterInfo...");
            ioEx.printStackTrace();
        }
    }

    private Record.Topic getRegisteredTopic(String topicName) {
        return topics.getOrDefault(topicName, null);
    }

    public UUID getRegisteredTopicID(String topicName) {
        Record.Topic topic = getRegisteredTopic(topicName);
        if (topic == null) {
            System.err.println("Topic " + topicName + " not found");
        }
        return topic != null ? topic.getTopicId() : null;
    }

    public List<DescribeTopicPartitionsResponse.Partition> getPartitionsOfTopic(UUID topicId) {
        List<DescribeTopicPartitionsResponse.Partition> res = new ArrayList<>();
        List<Record.Partition> partitionList = partitions.get(topicId);
        for (Record.Partition partition : partitionList) {
            res.add(new DescribeTopicPartitionsResponse.Partition((short) 0, partition.id(), partition.leader(), partition.leaderEpoch(),
                    partition.replicas(), partition.inSyncReplicas(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()));
        }
        return res;


    }
}
