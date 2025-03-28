
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static kafka.util.Utils.*;


public class Main {

    private static final int PORT = 9092;
    private static final int THREAD_POOL_SIZE = 4;
    public static final short API_VERSIONS = 18;
    public static final short MIN_API_VERSION = 0;
    public static final short MAX_API_VERSION = 4;
    public static final short DESCRIBE_TOPIC_PARTITIONS = 75;
    public static final short MIN_DESCRIBE_TOPIC_PARTITION = 0;
    public static final short MAX_DESCRIBE_TOPIC_PARTITION = 0;

    public static final byte TAG_BUFFER = 0;
    public static final short UNKNOWN_TOPIC_OR_PARTITION_ERR = 3;
    public static final short UNSUPPORTED_VERSION = 35;



    private static void listenToServerStream(Socket clientSocket){
        /*
        message_size: 4 bytes long
        Header
          request_api_key	INT16 (2 bytes)	The API key for the request
          request_api_version	INT16 (2 bytes)	The version of the API for the request
          correlation_id	INT32 (4 bytes)	A unique identifier for the request
          client_id	NULLABLE_STRING	The client ID for the request
          TAG_BUFFER	COMPACT_ARRAY	Optional tagged fields
        Body

        error_code => INT16
        */

//        byte [] buffer = new byte[1024];
        // Kafka API versions request (v4)
        int message_size;
        // RequestHeader
        short request_api_key;
        short request_api_version;

        int correlation_id;
        short client_id_length;
        String client_id;

        // DescribeTopicPartitions RequestBody
        byte topics_array_length = 1;  // The length of the topics array + 1

        // Error Response Body
        short error_code = 0;

        try (OutputStream outputStream =  clientSocket.getOutputStream() ){
            while ( true ) {
                DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                if (dataInputStream.available() == 0){
                    System.out.println("No data available. Wait for 500 ms... ");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        System.err.println(e.getMessage());
                    }
                    continue;
                }

                System.out.println("processing new request with len= " + dataInputStream.available());
                message_size = dataInputStream.readInt();
                System.out.println("message_size=" + message_size);
                request_api_key = dataInputStream.readShort();
                System.out.println("api_key=" + request_api_key);
                request_api_version = dataInputStream.readShort();
                System.out.println("api_version=" + request_api_version);
                correlation_id = dataInputStream.readInt();
                System.out.println("correlation_id=" + correlation_id);
                client_id_length = dataInputStream.readShort();
                System.out.println("client_id_length=" + client_id_length);
                if (client_id_length > 0) {
                    int available = Math.min(client_id_length, dataInputStream.available());
                    System.out.println("client_id available= " + available);
                    try {
                        client_id = new String(dataInputStream.readNBytes(available), StandardCharsets.UTF_8);
                        System.out.println("client_id=" + client_id);
                        if (dataInputStream.available() > 0) {
                            dataInputStream.skipBytes(1);
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading client_id " + e.getMessage());
                        System.err.println(Arrays.toString(e.getStackTrace()));
                    }
                }

                ByteArrayOutputStream descTopicPartitions = new ByteArrayOutputStream();
                if (dataInputStream.available() > 0){
                    topics_array_length = dataInputStream.readByte();
                    System.out.println("topics_array_length=" + topics_array_length);

                    for (byte b = 1; b < topics_array_length && dataInputStream.available() > 0; b++) {
                        byte topic_name_length = dataInputStream.readByte();

                        int available = Math.min(topic_name_length, dataInputStream.available());
                        System.out.println("topic_name available= " + available);
                        byte [] topic_name = dataInputStream.readNBytes(available);
                        String topicName = new String(topic_name, StandardCharsets.UTF_8);

                        System.out.println("topic_length=" + topic_name_length);
                        System.out.println("topicName=" + topicName);

                        descTopicPartitions.write(new byte[] {0, 3}); // error code
                        descTopicPartitions.write(topic_name_length);
                        descTopicPartitions.write(topic_name);

                        descTopicPartitions.write(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); // topic ID
                        descTopicPartitions.write(0); // is Internal
                        descTopicPartitions.write(1); // partitions array (1: empty)
                        descTopicPartitions.write(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0}); // topic authorized operations
                        descTopicPartitions.write(TAG_BUFFER);
                    }
                }

                ByteArrayOutputStream response_body_bytearray = new ByteArrayOutputStream();
                DataOutputStream response_body = new DataOutputStream(response_body_bytearray);
                response_body.writeInt(correlation_id);

                if (request_api_key == DESCRIBE_TOPIC_PARTITIONS &&
                        request_api_version == MIN_DESCRIBE_TOPIC_PARTITION) {
                    response_body.write(TAG_BUFFER);              // header tag buffer
                    response_body.writeInt(0);                 // throttle time
                    response_body.write(topics_array_length);     // topic array length
                    response_body.write(descTopicPartitions.toByteArray());
                    response_body.write(0xFF);                 // next cursor 0xFF (null) or 0x01 (not null)
                    response_body.write(TAG_BUFFER);
                    descTopicPartitions.flush();
                    descTopicPartitions.close();
                }
                else if ( request_api_key == API_VERSIONS &&
                        request_api_version >= MIN_API_VERSION &&
                        request_api_version <= MAX_API_VERSION ) {
                    response_body.writeShort(error_code);
                    // number of api keys
                    response_body.write(3);

                    response_body.writeShort(request_api_key);
                    response_body.writeShort(3);
                    response_body.writeShort(4);
                    response_body.write(TAG_BUFFER);

                    response_body.writeShort(DESCRIBE_TOPIC_PARTITIONS);
                    response_body.writeShort(0);
                    response_body.writeShort(0);
                    response_body.write(TAG_BUFFER);

                    response_body.writeInt(1);  // throttle time
                    response_body.write(TAG_BUFFER);

                } else {
                    error_code = UNSUPPORTED_VERSION;
                    System.out.println("error_code=" + error_code);
                    response_body.writeShort(error_code);
                    response_body.write(0x00);
                    response_body.writeInt(0);  // throttle time
                    response_body.write(TAG_BUFFER);
                }

                response_body.flush();
                System.out.println("response_body size=" + response_body_bytearray.size());
                outputStream.write(toByteArray(response_body_bytearray.size()));
//                System.out.println("response_body_bytearray=" + Arrays.toString(response_body_bytearray.toByteArray()));
                outputStream.write(response_body_bytearray.toByteArray());
                response_body.close();
                response_body_bytearray.flush();
                response_body_bytearray.close();

                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            System.err.println(Arrays.toString(e.getStackTrace()));
        }
    }

    public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        try (ServerSocket serverSocket = new ServerSocket(PORT)){

           // Since the tester restarts your program quite often, setting SO_REUSEADDR
           // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            System.out.println("Listening on port " + PORT);
           // Wait for connection from client.
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    executorService.submit(() -> listenToServerStream(clientSocket));
                } finally {
                    System.out.println("Closing server socket");
                }
//                try {
//                    if (clientSocket != null) {
//                        clientSocket.close();
//                    }
//                }
            }

        } catch (IOException ie) {
            System.out.println("IOException: " + ie.getMessage());
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        } finally {
            executorService.shutdown();
        }
    }
}
