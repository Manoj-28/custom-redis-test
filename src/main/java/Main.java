import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;

// Thread to handle client communication
class ClientHandler extends Thread {
    private Socket clientSocket;
    public static Map<String, String> KeyValueStore = new HashMap<>();

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
                System.out.println("Inputline: "+inputLine);
                if (inputLine == null) break;

                String[] commandParts = inputLine.split(" ");
                System.out.println("cd0"+commandParts[0]);
                String command = commandParts[0].toUpperCase();
                if (command.equals("PING")) {
                    out.write("+PONG\r\n".getBytes());
                } else if (command.equals("ECHO")) {
                    reader.readLine();
                    String message = reader.readLine();
                    out.write(String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
                } else if (command.equals("SET")) {
                    handleSetCommand(commandParts, out);
                } else if (command.equals(("GET"))) {
                    handleGetCommand(commandParts, out);
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

    private void handleSetCommand(String[] commandParts, OutputStream out) throws IOException {
        if (commandParts.length < 3) {
            out.write("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
            return;
        }
        String key = commandParts[1];
        String value = commandParts[2];

        KeyValueStore.put(key, value);

        out.write("+OK\r\n".getBytes());
    }
     private void handleGetCommand(String[] commandParts, OutputStream out) throws IOException{

        if(commandParts.length < 2){
            out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
            return;
        }

        String key = commandParts[1];
        String value = KeyValueStore.get(key);

        if(value != null){
            out.write(String.format("$%d\r\n%s\r\n", value.length(), value).getBytes());
        }
        else{
            out.write("$-1\r\n".getBytes());
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