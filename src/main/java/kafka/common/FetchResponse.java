package kafka.common;

import kafka.protocol.io.DataOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public record FetchResponse(short errorCode, int throttleTimeInMs, int sessionId, List<TopicResponses> topicResp) implements Body {
    public static final short API_KEY = 1;
    public static final short MIN_API_VERSION = 3;
    public static final short MAX_API_VERSION = 16;

    public void serialize(DataOutput output) {
        output.writeInt(throttleTimeInMs);
        output.writeShort(errorCode);
        output.writeInt(sessionId);
        output.writeCompactArray(topicResp, TopicResponses::serialize);

        output.skipEmptyTaggedFieldArray();
    }
    public record TopicResponses(UUID topicId, List<PartitionRecord> partitionRecords) {
        public void serialize(DataOutput output) {
            output.writeUuid(topicId);
            output.writeCompactArray(partitionRecords, PartitionRecord::serialize);

            output.skipEmptyTaggedFieldArray();
        }
    }

    public record PartitionRecord(short errorCode, int partitionId, byte[] recordBytes) {
        public void serialize(DataOutput output) {
            output.writeInt(partitionId);
            output.writeShort(errorCode);
            // high_watermark
            output.writeLong(0L);
            // last_stable_offset
            output.writeLong(0L);
            // logStartOffset
            output.writeLong(0L);
            // aborted_transactions
            output.writeCompactArray(new ArrayList<>(), Transaction::serialize);
            // preferred_read_replica
            output.writeInt(0);
            // record_set
            output.writeCompactBytes(recordBytes);

            output.skipEmptyTaggedFieldArray();
        }

    }

    public record Transaction() {
        void serialize(DataOutput output) {
            // producer Id
            output.writeLong(0L);
            // first_offset
            output.writeLong(0L);

//            output.skipEmptyTaggedFieldArray();
        }
    }

}
