import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    int fromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    Long fromByteArrayLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    private static void listenToServerStream(InputStream inputStream, OutputStream outputStream) throws IOException {
        /*
        message_size: 4 bytes long
        Header
          request_api_key	INT16 (2 bytes)	The API key for the request
          request_api_version	INT16 (2 bytes)	The version of the API for the request
          correlation_id	INT32 (4 bytes)	A unique identifier for the request
          client_id	NULLABLE_STRING	The client ID for the request
          TAG_BUFFER	COMPACT_ARRAY	Optional tagged fields
        Body
        */
        byte [] buffer = new byte[1024];
        byte [] message_size;
        byte [] request_api_key;
        byte [] request_api_version;
        byte [] correlation_id;
        int len;
        if ( (len = inputStream.read(buffer)) != -1 ){
            message_size = Arrays.copyOfRange(buffer, 0, 4);
            request_api_key = Arrays.copyOfRange(buffer, 4, 6);
            request_api_version = Arrays.copyOfRange(buffer, 6, 8);
            correlation_id = Arrays.copyOfRange(buffer, 8, 12);
            outputStream.write(message_size);
            outputStream.write(correlation_id);
        }

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
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream =  clientSocket.getOutputStream();
            listenToServerStream(inputStream, outputStream);
            outputStream.close();
//       outputStream.write(new byte[] {0,0,0,0,0,0,0,7});
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
