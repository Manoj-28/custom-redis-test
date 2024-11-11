import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;

class ValueWithExpiry{
    String value;
    long expiryTime;

    public ValueWithExpiry(String value,long expiryTime){
        this.value =value;
        this.expiryTime = expiryTime;
    }

    public boolean isExpired(){
        return expiryTime > 0 && System.currentTimeMillis() > expiryTime;
    }
}

// Thread to handle client communication
class ClientHandler extends Thread {
    private Socket clientSocket;
    public static Map<String, ValueWithExpiry> KeyValueStore = new HashMap<>();

    private static String dir;
    private static String dbfilename;
    private static boolean isReplica;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public static void setDir(String dirPath){
        dir = dirPath;
    }

    public static void setDbfilename(String filename){
        dbfilename = filename;
    }

    public static void setIsReplica(boolean replica) {
        isReplica = replica;
    }

    private void handleKeysCommand(String[] commandParts, OutputStream out) throws IOException {
        if (commandParts.length < 1){
            out.write("-ERR unsupported KEYS pattern\r\n".getBytes());
            return;
        }
        StringBuilder response = new StringBuilder();
        response.append("*").append(KeyValueStore.size()).append("\r\n");

        for (String key: KeyValueStore.keySet()){
//            if(!KeyValueStore.get(key).isExpired()){
            response.append(String.format("$%d\r\n%s\r\n", key.length(), key));
//            }
        }
        out.write(response.toString().getBytes());
    }

    private String[] parseRespCommand(BufferedReader reader, String firstLine) throws IOException{
        int numElements = Integer.parseInt(firstLine.substring(1));
        String[] commandParts = new String[numElements];

        for(int i=0;i<numElements;i++){
            String lengthLine = reader.readLine();
            if(lengthLine.startsWith("$")){
                String bulkString = reader.readLine();
                commandParts[i] = bulkString;
            }
        }
        System.out.println("Parsed RESP Command: " + String.join(", ", commandParts));
        return commandParts;
    }

    private void handleSetCommand(String[] commandParts, OutputStream out) throws IOException {
        if (commandParts.length < 3) {
            out.write("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
            return;
        }
        String key = commandParts[1];
        String value = commandParts[2];
        long expiryTime = -1;

        if(commandParts.length >= 5 && commandParts[3].equalsIgnoreCase("PX")){
            try{
                long expiryInMilliseconds = Long.parseLong(commandParts[4]);
                expiryTime = System.currentTimeMillis() + expiryInMilliseconds;
            }
            catch (NumberFormatException e){
                out.write("-ERR invalid PX argument\r\n".getBytes());
                return;
            }
        }

        KeyValueStore.put(key, new ValueWithExpiry(value,expiryTime));

        out.write("+OK\r\n".getBytes());
    }
    private void handleGetCommand(String[] commandParts, OutputStream out) throws IOException{

        if(commandParts.length < 2){
            out.write("-ERR wrong number of arguments for 'GET' command\r\n".getBytes());
            return;
        }

        String key = commandParts[1];
        ValueWithExpiry valueWithExpiry = KeyValueStore.get(key);

        if(valueWithExpiry != null){
            if(valueWithExpiry.isExpired()){
                KeyValueStore.remove(key);
                out.write("$-1\r\n".getBytes());
            }
            else{
                String value = valueWithExpiry.value;
                out.write(String.format("$%d\r\n%s\r\n", value.length(), value).getBytes());
            }
        }
        else{
            out.write("$-1\r\n".getBytes());
        }
    }

    public void handleConfigGetCommand(String[] commandParts, OutputStream out) throws IOException{
        if(commandParts.length < 2){
            out.write("-ERR wrong number of arguments for 'CONFIG GET' command\r\n".getBytes());
            return;
        }
        String configParam = commandParts[2].toLowerCase();
        String response;

        switch (configParam){
            case "dir":
                response = String.format("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", dir.length(), dir);
                out.write(response.getBytes());
                break;
            case "dbfilename":
                response = String.format("*2\r\n$9\r\ndbfilename\r\n$%d\r\n%s\r\n", dbfilename.length(), dbfilename);
                out.write(response.getBytes());
                break;
            default:
                out.write("-ERR unknown configuration parameter\r\n".getBytes());
        }
    }
    private void handleInfoCommand(String[] commandParts, OutputStream out) throws IOException {

        if(commandParts.length >= 2 && "replication".equalsIgnoreCase(commandParts[1])){

            String role = isReplica ? "slave" : "master";
            String infoResponse = "role:" + role;
            String bulkString = String.format("$%d\r\n%s\r\n", infoResponse.length(), infoResponse);
            out.write(bulkString.getBytes());
        }
        else {
            out.write("-ERR unsupported INFO section\r\n".getBytes());
        }
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream out = clientSocket.getOutputStream()
        ) {

            while (true) {
                String inputLine = reader.readLine();
                if (inputLine == null) break;

                if(inputLine.startsWith("*")){
                    String[] commandParts = parseRespCommand(reader, inputLine);
                    if(commandParts != null && commandParts.length > 0){
                        String command = commandParts[0].toUpperCase();

                        switch (command){
                            case "PING":
                                out.write("+PONG\r\n".getBytes());
                                break;
                            case "ECHO":
                                if(commandParts.length > 1){
                                    String message = commandParts[1];
                                    out.write(String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
                                }
                                break;
                            case "SET":
                                handleSetCommand(commandParts,out);
                                break;
                            case "GET":
                                handleGetCommand(commandParts, out);
                                break;
                            case "CONFIG":
                                handleConfigGetCommand(commandParts,out);
                                break;
                            case "KEYS":
                                handleKeysCommand(commandParts, out);
                                break;
                            case "INFO":
                                handleInfoCommand(commandParts,out);
                                break;
                            default:
                                out.write("-ERR unknown command\r\n".getBytes());
                        }
                    }
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
        int port = 6379;  // Default port
        String dir = "/tmp/redis-files";  // Default directory
        String dbfilename = "dump.rdb";   // Default DB filename

        // Parse the command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[i + 1]);
                        } catch (NumberFormatException e) {
                            System.out.println("Invalid port number. Using default port 6379.");
                        }
                    }
                    break;
                case "--dir":
                    if (i + 1 < args.length) {
                        dir = args[i + 1];
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        dbfilename = args[i + 1];
                    }
                    break;
                case "--replicaof":
                    ClientHandler.setIsReplica(true);
                    break;
            }
        }

        // Load the RDB file
        RdbParser.loadRDB(dir, dbfilename);

        // Set directory and filename for client handler
        ClientHandler.setDir(dir);
        ClientHandler.setDbfilename(dbfilename);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server started on port " + port + ", waiting for connections...");

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