import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    private static void listenToServerStream(Socket clientSocket) throws IOException {
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

        byte [] buffer = new byte[1024];
        byte [] message_size = new byte[4];  // response header
        byte [] request_api_key = new byte[2];
        byte [] request_api_version = new byte[2];
        short request_api_version_short;
        byte [] correlation_id = new byte[4]; // response body
        ByteArrayOutputStream response_body = new ByteArrayOutputStream();
        short error_code;
        int len;

        InputStream inputStream = clientSocket.getInputStream();
        try (OutputStream outputStream =  clientSocket.getOutputStream() ){
            if ( (len = inputStream.read(buffer)) != -1 ) {
                message_size = Arrays.copyOfRange(buffer, 0, 4);
                request_api_key = Arrays.copyOfRange(buffer, 4, 6);
                request_api_version = Arrays.copyOfRange(buffer, 6, 8);
                System.out.println("short api_version=" + (request_api_version_short = fromByteArray(request_api_version)));
                correlation_id = Arrays.copyOfRange(buffer, 8, 12);
                System.out.println("correlation_id byte array=" +  Arrays.toString(correlation_id));
                System.out.println("correlation_id=" + fromByteArrayLong(correlation_id));
                if (request_api_version_short < 0 || request_api_version_short > 4) {
                    error_code = 35;
                    response_body.write(shortToByteArray(error_code));
                }
                else {
                    error_code = 0;
                    response_body.write(shortToByteArray(error_code)); // array size + 1
                    response_body.write(2);
                    response_body.write(shortToByteArray((short) 18)); // api_key (RequestKey.API_VERSIONS.type)
                    response_body.write(shortToByteArray((short) 3));  // min version
                    response_body.write(shortToByteArray((short) 4));  // max version
                    response_body.write(0); // tagged fields
                    response_body.write(intToByteArray(1)); //throttle time
                    response_body.write(0); // tagged fields
                    System.out.println("response_body=" + Arrays.toString(response_body.toByteArray()));
                }

                int response_length = correlation_id.length + response_body.size();
                outputStream.write(intToByteArray(response_length));
                outputStream.write(correlation_id);
                outputStream.write(response_body.toByteArray());
                outputStream.flush();
            }
        }
        inputStream.close();
    }

    public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

//     Uncomment this block to pass the first stage
        ServerSocket serverSocket;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
           // Since the tester restarts your program quite often, setting SO_REUSEADDR
           // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
           // Wait for connection from client.
            clientSocket = serverSocket.accept();
            listenToServerStream(clientSocket);

       } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
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
