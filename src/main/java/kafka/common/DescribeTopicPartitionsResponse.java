package kafka.common;

import kafka.protocol.io.DataOutput;

import java.util.List;
import java.util.UUID;

public record DescribeTopicPartitionsResponse(List<DescribeTopicPartitionsResponse.Topic> topics) implements Body{
    public static final short API_KEY = DescribeTopicPartitionsRequest.API_KEY;
    public static final short MIN_API_VERSION = 0;
    public static final short MAX_API_VERSION = 0;

    public void serialize(DataOutput output) {
        output.writeInt(0); // throttle time
        output.writeCompactArray(topics, Topic::serialize);
        output.writeByte((byte) 0xFF);
        output.skipEmptyTaggedFieldArray();
    }

    public record Topic(short errorCode, String topicName, UUID topicId, List<DescribeTopicPartitionsResponse.Partition> partitions) {

        void serialize(DataOutput output) {
            output.writeShort(errorCode);
            output.writeCompactString(topicName);
            output.writeUuid(topicId);
            output.writeBoolean(false); // isInternal
            output.writeCompactArray(partitions, Partition::serialize);
            output.writeInt(0); // topic authorized operation

            output.skipEmptyTaggedFieldArray();

        }

    }

    public record Partition(short errorCode, int partitionIndex, int leaderId, int leaderEpoch, List<Integer> replicas,
                     List<Integer> inSyncReplicas, List<Integer> eligibleLeaderReplicas, List<Integer> lastKnownEligibleLeaderReplicas,
                     List<Integer> offlineReplicas) {
        void serialize(DataOutput output) {
            output.writeShort(errorCode);
            output.writeInt(partitionIndex);
            output.writeInt(leaderId);
            output.writeInt(leaderEpoch);
            output.writeCompactIntArray(replicas);
            output.writeCompactIntArray(inSyncReplicas);
            output.writeCompactIntArray(eligibleLeaderReplicas);
            output.writeCompactIntArray(lastKnownEligibleLeaderReplicas);
            output.writeCompactIntArray(offlineReplicas);
            output.skipEmptyTaggedFieldArray();

        }
    }




}
