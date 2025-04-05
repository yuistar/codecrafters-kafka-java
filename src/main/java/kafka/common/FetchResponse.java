package kafka.common;

import kafka.protocol.io.DataOutput;

import java.util.List;

public record FetchResponse(short errorCode, String topicName, int partitionId) implements Body {
    public static final short API_KEY = 1;
    public static final short MIN_API_VERSION = 3;
    public static final short MAX_API_VERSION = 16;

    public void serialize(DataOutput output) {
        output.writeShort(errorCode); // No Error

        output.skipEmptyTaggedFieldArray();
    }

    public record Key(short apiKey, short minVersion, short maxVersion){
        public void serialize(DataOutput output) {
            output.writeShort(apiKey);
            output.writeShort(minVersion);
            output.writeShort(maxVersion);
            output.skipEmptyTaggedFieldArray();

        }
    }
}
