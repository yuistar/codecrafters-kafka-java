package kafka.common;

import kafka.protocol.io.DataInput;

import java.util.List;
import java.util.UUID;

public record FetchRequest(List<Topic> topics) implements Request{

    public static final short API_KEY = 1;


    public static FetchRequest deserialize(DataInput input) {
        final var maxWaitMs = input.readSignedInt();
        final var minBytes = input.readSignedInt();
        final var maxBytes = input.readSignedInt();
        final var isolationLevel = input.readSignedByte();
        final var sessionId = input.readSignedInt();
        final var sessionEpoch = input.readSignedInt();
        final var topics = input.readCompactArray(FetchRequest.Topic::deserialize);
        input.skipEmptyTaggedFieldArray();
//        final var forgottenTopics = input.readCompactArray(FetchRequest.TopicData::deserialize);
//        input.skipEmptyTaggedFieldArray();
        final var rackId = input.readCompactString();

        return new FetchRequest(topics);
    }

    @Override
    public short getApiKey(){
        return API_KEY;
    }

    public record Topic(UUID topicId, List<Partition> topicPartitions){
        public static Topic deserialize(DataInput input) {
            final var topicId = input.readUuid();
            final var partitions = input.readCompactArray(FetchRequest.Partition::deserialize);

            System.out.println("parsed FetchRequest.Topic " + topicId);
            System.out.println("parsed FetchRequest.Partitions size " + partitions.size());
            return new Topic(topicId, partitions);
        }
    }

    public record Partition(int partitionId) {
        static Partition deserialize(DataInput input) {
            final var partitionId = input.readSignedInt();
            final var currentLeaderEpoch = input.readSignedInt();
            final var fetchOffset = input.readSignedLong();
            final var lastFetchedEpoch = input.readSignedInt();
            final var logStartOffset = input.readSignedLong();
            final var partitionMaxBytes = input.readSignedInt();
            System.out.println("deserialized partitionId= " + partitionId);

            return new Partition(partitionId);
        }
    }

    record TopicData(UUID topicId){
        static TopicData deserialize(DataInput input) {
            final var topicId = input.readUuid();
            final var partitionIds = input.readCompactArray(func -> {
                input.readSignedByte();
                return null;
            });

            return new TopicData(topicId);
        }
    }
}
