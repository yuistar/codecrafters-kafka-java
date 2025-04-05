package kafka;

import kafka.common.*;
import kafka.protocol.io.DataByteBuffer;
import kafka.protocol.io.DataInput;
import kafka.protocol.io.DataInputStream;
import kafka.protocol.io.DataOutput;
import kafka.protocol.io.DataOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.function.Function;


public class Client implements Runnable {

    public static final short UNKNOWN_TOPIC_OR_PARTITION_ERR = 3;
    public static final short UNSUPPORTED_VERSION = 35;
    public static final short UNKNOWN_TOPIC_ERR = 100;

    private final Socket clientSocket;
    private final DataInputStream dataInputStream;
    private final OutputStream outputStream;
    public Kafka kafka;

    public static Map<Short, Function<DataInput, ? extends Request>> deserializerMap = new HashMap<>();
    public static List<ApiVersionsResponseV4.Key> apiKeys = new ArrayList<>();
    static {
        deserializerMap.put(ApiVersionsRequestV4.API_KEY, ApiVersionsRequestV4::deserialize);
        deserializerMap.put(DescribeTopicPartitionsRequest.API_KEY, DescribeTopicPartitionsRequest::deserialize);
        deserializerMap.put(FetchRequest.API_KEY, FetchRequest::deserialize);

        apiKeys.add(new ApiVersionsResponseV4.Key(ApiVersionsResponseV4.API_KEY, ApiVersionsResponseV4.MIN_API_VERSION, ApiVersionsResponseV4.MAX_API_VERSION));
        apiKeys.add(new ApiVersionsResponseV4.Key(DescribeTopicPartitionsResponse.API_KEY, DescribeTopicPartitionsResponse.MIN_API_VERSION, DescribeTopicPartitionsResponse.MAX_API_VERSION));
        apiKeys.add(new ApiVersionsResponseV4.Key(FetchResponse.API_KEY, FetchResponse.MIN_API_VERSION, FetchResponse.MAX_API_VERSION));

    }

    public Client(Socket clientSocket, Kafka kafka) throws IOException {
        this.clientSocket = clientSocket;
        this.dataInputStream = new DataInputStream(clientSocket.getInputStream());
        this.outputStream = clientSocket.getOutputStream();
        this.kafka = kafka;
    }

    private Function<DataInput, ? extends Request> getDeserializer(Header.V2 header) {
        return deserializerMap.get(header.apiKey());
    }

    public void run() {
        System.out.println("connected: " + clientSocket.getRemoteSocketAddress());

        try (clientSocket) {
            while (clientSocket.isConnected()) {

                if (dataInputStream.available() < 0) {
                    System.out.println("No data available. Wait for 500 ms... ");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        System.err.println(e.getMessage());
                    }
                    continue;
                }

                final var message_size = dataInputStream.readSignedInt();
                if (message_size > 0) {
                    System.out.println("message_size=" + message_size);
                    final var request_buffer = dataInputStream.readNBytes(message_size);
                    DataByteBuffer request_input = new DataByteBuffer(request_buffer);
                    final var request_header = Header.V2.deserialize(request_input);
                    final var deserializer = getDeserializer(request_header);
                    final var request_body = deserializer.apply(request_input);
                    final var response = handle(request_header, request_body);
                    DataOutputStream response_output = new DataOutputStream(outputStream);
                    sendResponse(response_output, response);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (!(e instanceof EOFException)) {
                e.printStackTrace();
            }
        }
    }

    private Response handle(Header header, Request request) {
        return switch (request.getApiKey()) {
            case ApiVersionsRequestV4.API_KEY -> new Response(
                    new Header.V0(header.correlationId()),
                    handleApiVersionsRequest(header));

            case DescribeTopicPartitionsRequest.API_KEY -> new Response(
                    new Header.V1(header.correlationId()),
                    handleDescribeTopicPartitionsRequest(header, (DescribeTopicPartitionsRequest) request)
            );

            case FetchRequest.API_KEY -> new Response(
                    new Header.V1(header.correlationId()),
                    handleFetchRequest(header, (FetchRequest) request)
            );

            default -> null;
        };
    }

    private ApiVersionsResponseV4 handleApiVersionsRequest(Header header) {
        assert (header instanceof Header.V2);
        final short apiVersion = ((Header.V2) header).apiVersion();
        if ( apiVersion >= ApiVersionsResponseV4.MIN_API_VERSION && apiVersion <= ApiVersionsResponseV4.MAX_API_VERSION){
            return new ApiVersionsResponseV4(
                    (short) 0,
                    apiKeys,
                    1);
        } else {
            return new ApiVersionsResponseV4(
                    UNSUPPORTED_VERSION,
                    new ArrayList<>(),
                    0);
        }
    }

    private DescribeTopicPartitionsResponse handleDescribeTopicPartitionsRequest(Header header, DescribeTopicPartitionsRequest request){

        final short apiVersion = ((Header.V2) header).apiVersion();
        List<DescribeTopicPartitionsResponse.Topic> topics = new ArrayList<>();
        if ( apiVersion >= DescribeTopicPartitionsResponse.MIN_API_VERSION && apiVersion <= DescribeTopicPartitionsResponse.MAX_API_VERSION) {
            // check if topic name in system topics, get topic Id
            for (var topic : request.getTopics()) {
                UUID topicId = kafka.getRegisteredTopicID(topic.topicName());
                List<DescribeTopicPartitionsResponse.Partition> partitions = new ArrayList<>();
                if (topicId != null) {
                    // add all partitions of the topic
                    partitions = kafka.getPartitionsOfTopic(topicId);
                    topics.add(new DescribeTopicPartitionsResponse.Topic((short) 0, topic.topicName(), topicId, partitions));

                } else {
                    topics.add(new DescribeTopicPartitionsResponse.Topic(UNKNOWN_TOPIC_OR_PARTITION_ERR, topic.topicName(), new UUID(0, 0), partitions));
                }
            }

        } else {
            System.out.println("unsupport apiverion: " + apiVersion + " for apikey " + request.getApiKey());
        }
        return new DescribeTopicPartitionsResponse(topics);
    }

    private FetchResponse handleFetchRequest(Header header, FetchRequest request) {

        List<FetchResponse.TopicResponses> topicResp = new ArrayList<>();
        for (FetchRequest.Topic topic : request.topics()) {
            UUID topicId = topic.topicId();
            List<FetchResponse.PartitionRecord> partitionRecs = new ArrayList<>();
            if (kafka.isTopicIDRegistered(topicId)) {
                // add all partitions of the topic
//                partitions = kafka.getPartitionsOfTopic(topicId);
//                topics.add(new FetchResponse.TopicResponses((short) 0, topic.topicName(), topicId, partitions));
                System.out.println("[TODO] process responses for topic=" + topicId);

            } else {
                partitionRecs.add(new FetchResponse.PartitionRecord(UNKNOWN_TOPIC_ERR, 0));
                topicResp.add(new FetchResponse.TopicResponses(topicId, partitionRecs));
            }
        }
        return new FetchResponse((short) 0, 0, 0, topicResp);
    }

    private void sendResponse(DataOutput output, Response response){
        final var byteOutputStream = new ByteArrayOutputStream();

        final var temporaryOutput = new DataOutputStream(byteOutputStream);
        response.serialize(temporaryOutput);

        final var bytes = byteOutputStream.toByteArray();

        output.writeInt(bytes.length);
        output.writeRawBytes(bytes);
    }


//            ByteArrayOutputStream descTopicPartitions = new ByteArrayOutputStream();
//            if (dataInputStream.available() > 0){
//                topics_array_length = dataInputStream.readByte();
//                System.out.println("topics_array_length=" + topics_array_length);
//
//                for (byte b = 1; b < topics_array_length && dataInputStream.available() > 0; b++) {
//                    byte topic_name_length = dataInputStream.readByte();
//
//                    int available = Math.min(topic_name_length, dataInputStream.available());
//                    System.out.println("topic_name available= " + available);
//                    byte [] topic_name = dataInputStream.readNBytes(available);
//                    String topicName = new String(topic_name, StandardCharsets.UTF_8);
//
//                    System.out.println("topic_name_length=" + topic_name_length);
//                    System.out.println("topicName=" + topicName +".");
////                        if (isTopicExist(topicName)){
//                    descTopicPartitions.write(new byte[] {0, 0});
////                        } else {
////                            descTopicPartitions.write(new byte[] {0, 3}); // error code
////                        }
//
//                    descTopicPartitions.write(topic_name_length);
//                    descTopicPartitions.write(topic_name);
//
//                    descTopicPartitions.write(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); // topic ID
//                    descTopicPartitions.write(0); // is Internal
//                    descTopicPartitions.write(1); // partitions array (1: empty)
//                    descTopicPartitions.write(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0}); // topic authorized operations
//                    descTopicPartitions.write(TAG_BUFFER);
//                }
//            }
//
//            ByteArrayOutputStream response_body_bytearray = new ByteArrayOutputStream();
//            DataOutputStream response_body = new DataOutputStream(response_body_bytearray);
//            response_body.writeInt(correlation_id);
//
//            if (request_api_key == DESCRIBE_TOPIC_PARTITIONS &&
//                    request_api_version == MIN_DESCRIBE_TOPIC_PARTITION) {
//                response_body.write(TAG_BUFFER);              // header tag buffer
//                response_body.writeInt(0);                 // throttle time
//                response_body.write(topics_array_length);     // topic array length
//                response_body.write(descTopicPartitions.toByteArray());
//                response_body.write(0xFF);                 // next cursor 0xFF (null) or 0x01 (not null)
//                response_body.write(TAG_BUFFER);
//                descTopicPartitions.flush();
//                descTopicPartitions.close();
//            }
//            else if ( request_api_key == API_VERSIONS &&
//                    request_api_version >= MIN_API_VERSION &&
//                    request_api_version <= MAX_API_VERSION ) {
//                response_body.writeShort(error_code);
//                // number of api keys
//                response_body.write(3);
//
//                response_body.writeShort(request_api_key);
//                response_body.writeShort(3);
//                response_body.writeShort(4);
//                response_body.write(TAG_BUFFER);
//
//                response_body.writeShort(DESCRIBE_TOPIC_PARTITIONS);
//                response_body.writeShort(0);
//                response_body.writeShort(0);
//                response_body.write(TAG_BUFFER);
//
//                response_body.writeInt(1);  // throttle time
//                response_body.write(TAG_BUFFER);
//
//            } else {
//                error_code = UNSUPPORTED_VERSION;
//                System.out.println("error_code=" + error_code);
//                response_body.writeShort(error_code);
//                response_body.write(0x00);
//                response_body.writeInt(0);  // throttle time
//                response_body.write(TAG_BUFFER);
//            }
//
//            response_body.flush();
//            System.out.println("response_body size=" + response_body_bytearray.size());
//            outputStream.write(toByteArray(response_body_bytearray.size()));
////                System.out.println("response_body_bytearray=" + Arrays.toString(response_body_bytearray.toByteArray()));
//            outputStream.write(response_body_bytearray.toByteArray());
//            response_body.close();
//            response_body_bytearray.flush();
//            response_body_bytearray.close();
//
//            outputStream.flush();
//        }

}
