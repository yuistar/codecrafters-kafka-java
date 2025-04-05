package kafka.common;


import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataOutput;


public interface Header {

    int correlationId();
    void serialize(DataOutput output);

    record V0(int correlationId) implements Header{
        @Override
        public void serialize(DataOutput out) {
            out.writeInt(correlationId);
        }
    }

    record V1(int correlationId) implements Header{
        @Override
        public void serialize(DataOutput out) {
            out.writeInt(correlationId);
            out.skipEmptyTaggedFieldArray();
        }

        @Override
        public String toString() {
            return "Header.V1={correlationId = %d".formatted(correlationId);
        }
    }

    record V2(short apiKey, short apiVersion, int correlationId, String clientId) implements Header{

        @Override
        public void serialize(DataOutput out) {
            out.writeInt(correlationId);
            out.skipEmptyTaggedFieldArray();
        }

        public static V2 deserialize(DataInput input) {
            //        Header
            //        request_api_key	INT16 (2 bytes)	The API key for the request
            //        request_api_version	INT16 (2 bytes)	The version of the API for the request
            //        correlation_id	INT32 (4 bytes)	A unique identifier for the request
            //        client_id	NULLABLE_STRING	The client ID for the request
            //        TAG_BUFFER	COMPACT_ARRAY	Optional tagged fields
            final var apiKey = input.readSignedShort();
            final var apiVersion = input.readSignedShort();
            final var correlationId = input.readSignedInt();
            final var clientId = input.readString();
            input.skipEmptyTaggedFieldArray();
            Header.V2 headerV2 = new V2(apiKey, apiVersion, correlationId, clientId);
            System.out.println(headerV2);
            return headerV2;
        }

        @Override
        public String toString() {
            return "Header.V2={api_key = %d, api_version = %d, correlationId = %d, clientId = %s"
                    .formatted(apiKey, apiVersion, correlationId, clientId);
        }
    }
}
