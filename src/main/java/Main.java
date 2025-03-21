import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    static short fromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    static byte[] shortToByteArray(short number) {
        return ByteBuffer.allocate(2).putShort(number).array();
    }

    static byte[] intToByteArray(Integer number) {
        return ByteBuffer.allocate(4).putInt(number).array();
    }

    static Integer fromByteArrayLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private static void handleSequentialRequests(DataInputStream input, OutputStream outputStream) throws IOException, InterruptedException {
        /*
        Request:
            message_size: 4 bytes long
            Header
              request_api_key	INT16 (2 bytes)	The API key for the request
              request_api_version	INT16 (2 bytes)	The version of the API for the request
              correlation_id	INT32 (4 bytes)	A unique identifier for the request
              client_id	NULLABLE_STRING	The client ID for the request
              TAG_BUFFER	COMPACT_ARRAY	Optional tagged fields
        Response:
            message_size:
            Header
                correlation_id
            Body
                error_code => INT16
        */

        try {
//            System.out.println("Handle request from " + input.available());
            int message_size = input.readInt();  // request header
            System.out.println("request message_size=" + message_size);

            short request_api_key = input.readShort();
            System.out.println("api_key=" + request_api_key);
            short request_api_version = input.readShort();
            System.out.println("api_version=" + request_api_version);
            int correlation_id = input.readInt(); // will be in response header
            System.out.println("correlation_id=" + correlation_id);
            byte[] request_body = new byte[message_size - 8];
            System.out.println("request_body len=" + request_body.length);
            input.readFully(request_body);
            System.out.println("request_body=" + Arrays.toString(request_body));

            short error_code;
            ByteArrayOutputStream response_body = new ByteArrayOutputStream();
            if (request_api_version < 0 || request_api_version > 4) {
                error_code = 35;
                response_body.write(shortToByteArray(error_code));
            } else {
                error_code = 0;
                response_body.write(shortToByteArray(error_code));
                response_body.write(2); // array size + 1
                response_body.write(request_api_key); // api_key (RequestKey.API_VERSIONS.type)
                response_body.write(shortToByteArray((short) 3));  // min version
                response_body.write(shortToByteArray((short) 4));  // max version
                response_body.write(0); // tagged fields
                response_body.write(intToByteArray(1)); //throttle time
                response_body.write(0); // tagged fields

            }
            System.out.println("response_body=" + Arrays.toString(response_body.toByteArray()));
            int response_length = 4 + response_body.size();
            System.out.println("response_size=" + response_length);

            outputStream.write(intToByteArray(response_length));
            outputStream.write(correlation_id);
            outputStream.write(response_body.toByteArray());
            outputStream.flush();

            System.out.println("end processing a request");
        } catch (EOFException e) {
            System.err.println("EOF before reading all bytes" + e.getMessage());
        }
    }

    public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
        ServerSocket serverSocket;
        Socket clientSocket = null;
        final int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
           // Since the tester restarts your program quite often, setting SO_REUSEADDR
           // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
           // Wait for connection from client.
            clientSocket = serverSocket.accept();
            System.out.println("client is connected= " + clientSocket.isConnected());
            int counter = 0;
            while (true) {
                DataInputStream input = new DataInputStream(clientSocket.getInputStream());
                OutputStream output = clientSocket.getOutputStream();

                if (input.available() == 0) {
                    System.out.println("No data available");
                    Thread.sleep(50);
                    continue;
                }
                System.out.println("counter = " + counter);
//                clientSocket.setReceiveBufferSize(1024);
                System.out.println("client buffer size =" + clientSocket.getReceiveBufferSize());
                handleSequentialRequests(input, output);
                counter++;
            }
//            outputStream.flush();
//            dataInputStream.close();
        } catch (IOException e) {
            System.out.println("Main IOException: " + e.getMessage());
        } catch (InterruptedException ie) {
            System.out.println("Main InterruptedException: " + ie.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
