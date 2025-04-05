
import kafka.Client;
import kafka.Kafka;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;


public class Main {
    private static final int PORT = 9092;

    public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");
        Kafka kafka = new Kafka();
        try (ServerSocket serverSocket = new ServerSocket(PORT)){

           // Since the tester restarts your program quite often, setting SO_REUSEADDR
           // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            System.out.println("Listening on port " + PORT);
           // Wait for connection from client.
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                Thread.ofVirtual().start(new Client(clientSocket, kafka));
            }

        } catch (IOException ie) {
            System.out.println("IOException: " + ie.getMessage());
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
    }
}
