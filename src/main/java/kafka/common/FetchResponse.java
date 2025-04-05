package kafka.common;

import kafka.protocol.io.DataOutput;

import java.util.ArrayList;
import java.util.List;

public record FetchResponse(short errorCode, int throttleTimeInMs, int sessionId) implements Body {
    public static final short API_KEY = 1;
    public static final short MIN_API_VERSION = 3;
    public static final short MAX_API_VERSION = 16;

    public void serialize(DataOutput output) {
        output.writeInt(throttleTimeInMs);
        output.writeShort(errorCode);
        output.writeInt(sessionId);
        output.writeCompactArray(new ArrayList<>(), TopicResponses::serialize);

        output.skipEmptyTaggedFieldArray();
    }
    public record TopicResponses() {
        public void serialize(DataOutput output) {
            System.out.println("[TODO]: add topic partition responses");
        }
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
