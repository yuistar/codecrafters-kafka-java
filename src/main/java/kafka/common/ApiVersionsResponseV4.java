package kafka.common;

import kafka.protocol.io.DataOutput;

import java.util.List;

public record ApiVersionsResponseV4 (short errorCode, List<Key> apiKeys, Integer throttleTime) implements Body {
    public static final short API_KEY = ApiVersionsRequestV4.API_KEY;
    public static final short MIN_API_VERSION = 3;
    public static final short MAX_API_VERSION = 4;

    public void serialize(DataOutput output) {
        output.writeShort(errorCode); // No Error
        output.writeCompactArray(apiKeys, ApiVersionsResponseV4.Key::serialize);
        output.writeInt(throttleTime);
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
