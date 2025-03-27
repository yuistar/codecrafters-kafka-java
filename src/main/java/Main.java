import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final int PORT = 9092;
    private static final int THREAD_POOL_SIZE = 4;
    static short fromByteArray(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    static byte[] toByteArray(short number) {
        return ByteBuffer.allocate(2).putShort(number).array();
    }

    static byte[] toByteArray(int number) {
        return ByteBuffer.allocate(4).putInt(number).array();
    }

    static Integer fromByteArrayInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

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

        byte [] buffer = new byte[1024];
        byte [] message_size;
        byte [] request_api_key;
        byte [] request_api_version;
        short request_api_version_short;
        byte [] correlation_id;
        short error_code = 0;

        int len;
        try (OutputStream outputStream =  clientSocket.getOutputStream() ){
            InputStream inputStream = clientSocket.getInputStream();
            while ( (len = inputStream.read(buffer)) != -1 ) {
                System.out.println("read inputStream len=" + len);
                message_size = Arrays.copyOfRange(buffer, 0, 4);
                System.out.println("message_size=" + fromByteArray(message_size));
                request_api_key = Arrays.copyOfRange(buffer, 4, 6);
                System.out.println("api_key=" + fromByteArray(request_api_key));
                request_api_version = Arrays.copyOfRange(buffer, 6, 8);
                System.out.println("short api_version=" + (request_api_version_short = fromByteArray(request_api_version)));
                correlation_id = Arrays.copyOfRange(buffer, 8, 12);
                System.out.println("correlation_id=" + fromByteArrayInt(correlation_id));

                ByteArrayOutputStream response_body = new ByteArrayOutputStream();
                response_body.write(correlation_id);
                if (request_api_version_short < 0 || request_api_version_short > 4) {
                    // write error code
                    error_code = (short) 35;
                    System.out.println("error_code=" + Arrays.toString(toByteArray(error_code)));
                    response_body.write(toByteArray(error_code));
                }
                else {
                    response_body.write(toByteArray(error_code));
                    response_body.write(3); // number of api keys
                    response_body.write(request_api_key);
                    response_body.write(toByteArray((short) 3));
                    response_body.write(toByteArray((short) 4));
                    response_body.write(0);
                    response_body.write(new byte[] {0, 75});
                    response_body.write(toByteArray((short) 0));
                    response_body.write(toByteArray((short) 0));
                    response_body.write(0);
                    response_body.write(new byte[] {0, 0, 0, 1});
                    response_body.write(0);
                }

                outputStream.write(toByteArray(response_body.size()));
                outputStream.write(response_body.toByteArray());

                outputStream.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

//     Uncomment this block to pass the first stage
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
