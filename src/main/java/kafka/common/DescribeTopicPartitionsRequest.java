package kafka.common;

import kafka.protocol.io.DataInput;

import java.util.List;

public record DescribeTopicPartitionsRequest (List<Topic> topics) implements Request{

    public static final short API_KEY = 75;

    public static DescribeTopicPartitionsRequest deserialize(DataInput input) {
        final var topics = input.readCompactArray(DescribeTopicPartitionsRequest.Topic::deserialize);

        return new DescribeTopicPartitionsRequest(topics);
    }

    @Override
    public short getApiKey(){
        return API_KEY;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public record Topic(String topicName) {

        static Topic deserialize(DataInput input) {
            final var topicName = input.readCompactString();
            System.out.println("Describe topicName: " + topicName);
            input.skipEmptyTaggedFieldArray();

            return new Topic(topicName);
        }

    }
}
