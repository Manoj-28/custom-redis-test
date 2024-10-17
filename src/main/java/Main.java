import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

// Thread to handle client communication
class ClientHandler extends Thread {
    private Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream out = clientSocket.getOutputStream()
        ) {

            while (true) {
                String inputLine = reader.readLine();
                if (inputLine.equals("PING")) {
                    out.write("+PONG\r\n".getBytes());
                }
                else if(inputLine.equalsIgnoreCase("ECHO")){
//                    reader.readLine();
                    String message = reader.readLine();
                    out.write(String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException in client handler: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException when closing client socket: " + e.getMessage());
            }
        }
    }
}

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            System.out.println("Server started, waiting for connections...");

            while (true) {
                // Accept the client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");

                // Create a new thread to handle the client
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                clientHandler.start();  // Start the thread for this client
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
