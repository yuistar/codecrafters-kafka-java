package kafka.common;

import kafka.protocol.io.DataInput;

public record ApiVersionsRequestV4 (String clientId, String clientSoftwareVersion) implements Request{

    public static final short API_KEY = 18;


    public static ApiVersionsRequestV4 deserialize(DataInput input) {
        final var clientId = input.readCompactString();
        final var clientSoftwareVersion = input.readCompactString();
        input.skipEmptyTaggedFieldArray();

        return new ApiVersionsRequestV4(clientId, clientSoftwareVersion);
    }

    @Override
    public short getApiKey(){
        return API_KEY;
    }

}
