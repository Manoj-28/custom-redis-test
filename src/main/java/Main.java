import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;


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

class StreamEntry{
    String id;
    Map<String,String> fields;

    public StreamEntry(String id,Map<String,String> fields){
        this.id = id;
        this.fields = fields;
    }
}

// Thread to handle client communication
class ClientHandler extends Thread {
    private final Socket clientSocket;
    public static Map<String, ValueWithExpiry> KeyValueStore = new HashMap<>();
    private static List<Socket> replicas = new CopyOnWriteArrayList<>();

    private static String dir;
    private static String dbfilename;
    private static boolean isReplica;

    // Hardcoded replication ID and offset
    private static final String REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final long REPLICATION_OFFSET = 0;
    static final Map<Long, Integer> replicaAcknowledgment = new HashMap<>();
    static final Object waitLock = 0;
    static long currentOffset = 0;
    boolean ACKFlag = false;

    private static final Map<String,List<StreamEntry>> streams = new HashMap<>();

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
            response.append(String.format("$%d\r\n%s\r\n", key.length(), key));
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
        System.out.println("Master: Parsed RESP Command: " + String.join(", ", commandParts));
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

        String respCommand = String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", key.length(), key, value.length(), value);
//        String ackCommand  = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        currentOffset=0;
        synchronized (waitLock){
            replicaAcknowledgment.put(currentOffset,0);
        }

        for(Socket replicaSocket : replicas){
            try{
                OutputStream replicaOut = replicaSocket.getOutputStream();
                replicaOut.write(respCommand.getBytes());
//                replicaOut.write(ackCommand.getBytes());
//                System.out.println("getack send to replica");
                replicaOut.flush();
            }
            catch (IOException e){
                System.out.println("Failed to send commands to replica: "  +e.getMessage());
            }
        }
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

        if (commandParts.length >= 2 && "replication".equalsIgnoreCase(commandParts[1])) {
            String role = isReplica ? "slave" : "master";
            String infoResponse = String.format(
                    "role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
                    role, REPLICATION_ID, REPLICATION_OFFSET
            );
            String bulkString = String.format("$%d\r\n%s\r\n", infoResponse.length(), infoResponse);
            out.write(bulkString.getBytes());
        } else {
            out.write("-ERR unsupported INFO section\r\n".getBytes());
        }
    }

    static void handleReplicaAck(long offset){
        synchronized (waitLock){
            replicaAcknowledgment.computeIfPresent(offset,(key,value) -> value+1);
            waitLock.notifyAll();
        }
        for(Long key: replicaAcknowledgment.keySet()){
            System.out.println(key + "->" + replicaAcknowledgment.get(key));
        }
    }

    private void handleReplConfCommand(String[] commandParts, OutputStream out) throws IOException{
        if(commandParts[1].equals("listening-port")){
//            currentOffset++;
            out.write("+OK\r\n".getBytes());
        } else if (commandParts[1].equalsIgnoreCase("capa")) {

            handleReplicaAck(currentOffset);
            out.write("+OK\r\n".getBytes());
        } else if(commandParts[1].equalsIgnoreCase("ACK")){
                long ackOffset = currentOffset;
                handleReplicaAck(ackOffset);
//                out.write("+OK\r\n".getBytes());
        }
        else {
            out.write("-ERR wrong number of arguments for 'REPLCONF' command\r\n".getBytes());
        }
    }

    private byte[] getEmptyRDBFileContent(){
        String base64RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        return Base64.getDecoder().decode(base64RDB);
    }

    private void sendEmptyRDBFile(OutputStream out) throws IOException{
        byte[] rdbContent = getEmptyRDBFileContent();
        int length = rdbContent.length;
        String header = String.format("$%d\r\n", length);

        out.write(header.getBytes());
        out.write(rdbContent);
        out.flush();
    }

    private void handlePsyncCommand(String[] commandParts, OutputStream out) throws IOException{
        if(commandParts.length != 3){
            out.write("-ERR wrong number of arguments for 'PSYNC' command\r\n".getBytes());
            return;
        }
        String psyncResponse = String.format("+FULLRESYNC %s %d\r\n", REPLICATION_ID, REPLICATION_OFFSET);
        out.write(psyncResponse.getBytes());

        sendEmptyRDBFile(out);
    }
    private void handleWaitCommand(String[] commandParts, OutputStream out) throws IOException {
        if (commandParts.length < 3) {
            out.write("-ERR wrong number of arguments for 'WAIT' command\r\n".getBytes());
            return;
        }
        try {
            // Parse the arguments (numreplicas and timeout)
            int numReplicas = Integer.parseInt(commandParts[1]);
            int timeout = Integer.parseInt(commandParts[2]);

            long startTime = System.currentTimeMillis();
            int acknowledged = 0;
            String ackCommand  = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
            for(Socket replicaSocket : replicas){
                try{
                    OutputStream replicaOut = replicaSocket.getOutputStream();
                    replicaOut.write(ackCommand.getBytes());
                    System.out.println("getack send to replica");
                    replicaOut.flush();
                }
                catch (IOException e){
                    System.out.println("Failed to send commands to replica: "  +e.getMessage());
                }
            }

            synchronized (waitLock){
                while (System.currentTimeMillis() - startTime < timeout && acknowledged < numReplicas){
                    acknowledged = replicaAcknowledgment.values().stream().mapToInt(Integer::intValue).sum();
                    for(Long key: replicaAcknowledgment.keySet()){
                        System.out.println(key + "->" + replicaAcknowledgment.get(key));
                    }
                    System.out.println("CurrentOffset: " + currentOffset);
                    System.out.println("Acknowledged: " + acknowledged);
                    if(acknowledged < numReplicas){
                        waitLock.wait(timeout);
                    }
                }
            }

            out.write(String.format(":%d\r\n", acknowledged).getBytes());
        } catch (NumberFormatException | InterruptedException e) {
            out.write("-ERR invalid arguments for 'WAIT' command\r\n".getBytes());
        }
    }

    private void handleTypeCommand(String[] commandParts, OutputStream out) throws IOException {
        if(commandParts.length < 2){
            out.write("-ERR wrong number of arguments for 'TYPE' command\r\n".getBytes());
            return;
        }

        String key = commandParts[1];
        ValueWithExpiry value = KeyValueStore.get(key);
        if(value != null){
            out.write("+string\r\n".getBytes());
        }
        else{
            if(streams.containsKey(key)){
                out.write("+stream\r\n".getBytes());
            }
            else{
                out.write("+none\r\n".getBytes());
            }
        }
    }

    private void handleXReadCommand(String[] commandParts, OutputStream out) throws IOException {
        if (commandParts.length < 4 || !"streams".equals(commandParts[1])) {
            out.write("-ERR wrong number of arguments for 'XREAD' command\r\n".getBytes());
            return;
        }

        // Extract stream keys and corresponding start IDs
        int streamsIndex = 3;
        int idsIndex = streamsIndex + 1;
        int numStreams = (commandParts.length - streamsIndex) / 2;

        List<String> streamKeys = new ArrayList<>();
        List<String> startIds = new ArrayList<>();

        for (int i = 0; i < numStreams; i++) {
            streamKeys.add(commandParts[streamsIndex + i]);
            startIds.add(commandParts[idsIndex + i]);
        }

        StringBuilder response = new StringBuilder();
        response.append("*").append(numStreams).append("\r\n"); // Number of streams in the response

        for (int i = 0; i < numStreams; i++) {
            String streamKey = streamKeys.get(i);
            String startId = startIds.get(i);

            // Check if the stream exists
            List<StreamEntry> stream = streams.get(streamKey);
            if (stream == null || stream.isEmpty()) {
                // Add an empty response for the stream
                response.append("*2\r\n");
                response.append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n");
                response.append("*0\r\n"); // No entries
                continue;
            }

            // Parse start ID
            long startMillis = 0;
            long startSeq = 0;
            if (!"0-0".equals(startId)) {
                String[] startParts = startId.split("-");
                startMillis = Long.parseLong(startParts[0]);
                startSeq = startParts.length > 1 ? Long.parseLong(startParts[1]) : 0;
            }

            // Filter entries with ID strictly greater than startId
            List<StreamEntry> result = new ArrayList<>();
            for (StreamEntry entry : stream) {
                String[] idParts = entry.id.split("-");
                long entryMillis = Long.parseLong(idParts[0]);
                long entrySeq = Long.parseLong(idParts[1]);

                if (entryMillis > startMillis || (entryMillis == startMillis && entrySeq > startSeq)) {
                    result.add(entry);
                }
            }

            // Add the stream's data to the response
            response.append("*2\r\n");
            response.append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n");
            response.append("*").append(result.size()).append("\r\n");

            for (StreamEntry entry : result) {
                response.append("*2\r\n");
                response.append("$").append(entry.id.length()).append("\r\n").append(entry.id).append("\r\n");
                response.append("*").append(entry.fields.size() * 2).append("\r\n");
                for (Map.Entry<String, String> field : entry.fields.entrySet()) {
                    response.append("$").append(field.getKey().length()).append("\r\n")
                            .append(field.getKey()).append("\r\n");
                    response.append("$").append(field.getValue().length()).append("\r\n")
                            .append(field.getValue()).append("\r\n");
                }
            }
        }

        out.write(response.toString().getBytes());
    }



    private void handleXRangeCommand(String[] commndParts, OutputStream out) throws IOException {
        if(commndParts.length != 4){
            out.write("-ERR wrong number of arguments for 'XRANGE' command\r\n".getBytes());
            return;
        }
        String streamKey = commndParts[1];
        String startId = commndParts[2];
        String endId = commndParts[3];

        List<StreamEntry> stream = streams.get(streamKey);
        if(stream == null || stream.isEmpty()){
            out.write("*0\r\n".getBytes());
            return;
        }

        long startMillis = 0;
        long startSeq = 0;
        if (!"-".equals(startId)) {
            String[] startParts = startId.split("-");
            startMillis = Long.parseLong(startParts[0]);
            startSeq = startParts.length > 1 ? Long.parseLong(startParts[1]) : 0;
        }

        long endMillis = Long.MAX_VALUE;
        long endSeq = Long.MAX_VALUE;
        if (!"+".equals(endId)) {
            String[] endParts = endId.split("-");
            endMillis = Long.parseLong(endParts[0]);
            endSeq = endParts.length > 1 ? Long.parseLong(endParts[1]) : Long.MAX_VALUE;
        }

        List<StreamEntry> result = new ArrayList<>();
        for(StreamEntry entry : stream){
            String[] entryIdParts = entry.id.split("-");
            long entryMillis = Long.parseLong(entryIdParts[0]);
            long entrySeq = Long.parseLong(entryIdParts[1]);

            if((entryMillis > startMillis || (entryMillis == startMillis && entrySeq >= startSeq)) &&
                    (entryMillis < endMillis || (entryMillis == endMillis && entrySeq <= endSeq))){
                result.add(entry);
            }
        }

        StringBuilder response = new StringBuilder();
        response.append("*").append(result.size()).append("\r\n");
        for(StreamEntry entry : result){
            response.append("*2\r\n");
            response.append("$").append(entry.id.length()).append("\r\n").append(entry.id).append("\r\n");

            //key-value pairs
            response.append("*").append(entry.fields.size()*2).append("\r\n");
            for(Map.Entry<String, String> field : entry.fields.entrySet()){
                response.append("$").append(field.getKey().length()).append("\r\n").append(field.getKey()).append("\r\n");
                response.append("$").append(field.getValue().length()).append("\r\n").append(field.getValue()).append("\r\n");
            }
        }

        out.write(response.toString().getBytes());
    }

    private boolean isValidEntryId(String entryId){
        return entryId.matches("\\d+-\\d+");
    }



    private void handleXAddCommand(String[] commandParts, OutputStream out) throws IOException{
        if(commandParts.length < 5 || commandParts.length % 2 ==0){
            out.write("-ERR wrong number of arguments for 'XADD' command\r\n".getBytes());
            return;
        }

        String streamKey = commandParts[1];
        String entryId = commandParts[2];
        Map<String,String> fields = new HashMap<>();
        for(int i=3;i<commandParts.length;i=i+2){
            fields.put(commandParts[i],commandParts[i+1]);
        }

        streams.putIfAbsent(streamKey, new ArrayList<>());
        List<StreamEntry> stream = streams.get(streamKey);

        if(entryId.endsWith("*")) {
            String[] idParts = entryId.split("-");
            long millisecondsTime=0;
            if(idParts.length > 1){
                millisecondsTime = Long.parseLong(idParts[0]);
            }
            else {
                millisecondsTime = System.currentTimeMillis();
            }
            long sequenceNumber = 0;

            if(!stream.isEmpty()){
                StreamEntry lastEntry = stream.get(stream.size()-1);
                String[] lastIdParts = lastEntry.id.split("-");
                long lastMillisecondsTime = Long.parseLong(lastIdParts[0]);
                long lastSequenceNumber = Long.parseLong(lastIdParts[1]);

                if(millisecondsTime == lastMillisecondsTime){
                    sequenceNumber = lastSequenceNumber + 1;
                }
            }

            if(millisecondsTime == 0 && sequenceNumber<1){
                sequenceNumber=1;
            }

            entryId = millisecondsTime + "-" + sequenceNumber;

        } else if (!isValidEntryId(entryId)) {
            out.write("-ERR Invalid entry ID format\r\n".getBytes());
            return;
        }

        String[] idParts = entryId.split("-");
        long millisecondsTime = Long.parseLong(idParts[0]);
        long sequenceNumber = Long.parseLong(idParts[1]);

        if(millisecondsTime == 0 && sequenceNumber < 1){
            out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
            return;
        }

        if(!stream.isEmpty()){
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            String[] lastIdParts = lastEntry.id.split("-");
            long lastMilliSecondsTime = Long.parseLong(lastIdParts[0]);
            long lastSequenceNumber = Long.parseLong(lastIdParts[1]);

            if(millisecondsTime < lastMilliSecondsTime || millisecondsTime == lastMilliSecondsTime && sequenceNumber <= lastSequenceNumber){
                out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".getBytes());
                return;
            }
        }

        stream.add(new StreamEntry(entryId,fields));
        out.write(String.format("$%d\r\n%s\r\n", entryId.length(), entryId).getBytes());
    }

    @Override
    public void run() {
        boolean isReplicaConnection = false;
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
                                ACKFlag=true;
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
                            case "REPLCONF":
                                handleReplConfCommand(commandParts,out);
                                break;
                            case "WAIT":
                                handleWaitCommand(commandParts, out);
                                break;
                            case "PSYNC":
                                isReplicaConnection = true;
                                replicas.add(clientSocket);         //add replica socket
                                handlePsyncCommand(commandParts,out);
                                break;
                            case "TYPE":
                                handleTypeCommand(commandParts,out);
                                break;
                            case "XADD":
                                handleXAddCommand(commandParts,out);
                                break;
                            case "XRANGE":
                                handleXRangeCommand(commandParts,out);
                                break;
                            case "XREAD":
                                handleXReadCommand(commandParts,out);
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
            if(clientSocket != null){
                try{
                    clientSocket.close();
                }
                catch (IOException e){
                    System.out.println("IOException when closing client socket: " + e.getMessage());
                }
                if(isReplicaConnection){
                    replicas.remove(clientSocket);      //remove replicas from list
                }
            }
        }
    }
}

public class Main {

    private static long offset = 0;

    public static void main(String[] args) {
        int port = 6379;  // Default port
        String dir = "/tmp/redis-files";  // Default directory
        String dbfilename = "dump.rdb";   // Default DB filename
        String masterHost="";
        int masterPort=-1;
        boolean isReplica=false;
        synchronized (ClientHandler.waitLock){
            ClientHandler.replicaAcknowledgment.put(ClientHandler.currentOffset,0);
        }

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
                    if(i+1 < args.length){
                        String[] hostAndPort = args[i+1].split(" ");
                        if(hostAndPort.length == 2){
                            masterHost = hostAndPort[0];
                            try {
                                masterPort = Integer.parseInt(hostAndPort[1]);
                                isReplica = true;
                            }
                            catch (NumberFormatException e){
                                System.out.println("Invalid master port number.");
                            }
                        }
                        else{
                            System.out.println("Invalid format for --replicaof. Expected: \"<host> <port>\"");
                        }
                    }
                    break;
            }
        }

        // Load the RDB file
        RdbParser.loadRDB(dir, dbfilename);

        ClientHandler.setDir(dir);
        ClientHandler.setDbfilename(dbfilename);
        ClientHandler.setIsReplica(isReplica);

        if(isReplica && masterHost != null && masterPort > 0){
            final String finalMasterHost = masterHost;
            final int finalMasterPort = masterPort;
            int finalReplicaPort = port;
            new Thread(() -> connectToMaster(finalMasterHost, finalMasterPort, finalReplicaPort)).start();
        }

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

    public static void connectToMaster(String masterHost, int masterPort, int replicaPort) {
        try (Socket masterSocket = new Socket(masterHost, masterPort);
             OutputStream out = masterSocket.getOutputStream();
             InputStream in = masterSocket.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {  // Use BufferedReader to read lines

            System.out.println("Connected to master at " + masterHost + ":" + masterPort);

            // Step 1: Send PING command
            String pingCommand = "*1\r\n$4\r\nPING\r\n";
            out.write(pingCommand.getBytes());
            out.flush();
            System.out.println("Sent PING to master");

            String pingResponse = reader.readLine();  // Use BufferedReader to read the response line
            if (!"+PONG".equals(pingResponse)) {
                System.out.println("Unexpected response to PING: " + pingResponse);
                return;
            }

            // Step 2: Send REPLCONF listening-port
            String replConfListeningPort = String.format("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", replicaPort);
            out.write(replConfListeningPort.getBytes());
            out.flush();
            System.out.println("Sent REPLCONF listening-port to master");

            String replConfListeningPortResponse = reader.readLine();
            if (!"+OK".equals(replConfListeningPortResponse)) {
                System.out.println("Unexpected response to REPLCONF listening-port: " + replConfListeningPortResponse);
                return;
            }

            // Step 3: Send REPLCONF capa psync2
            String replConfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            out.write(replConfCapa.getBytes());
            out.flush();
            System.out.println("Sent REPLCONF capa psync2 to master");

            String replConfCapaResponse = reader.readLine();
            if (!"+OK".equals(replConfCapaResponse)) {
                System.out.println("Unexpected response to REPLCONF capa psync2: " + replConfCapaResponse);
                return;
            }

            // Step 4: Send PSYNC command
            String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            out.write(psyncCommand.getBytes());
            out.flush();
            System.out.println("Sent PSYNC ? -1 to master");

            // Read the FULLRESYNC response
            String psyncResponse = reader.readLine();
            if (psyncResponse != null && psyncResponse.startsWith("+FULLRESYNC")) {
                System.out.println("Received FULLRESYNC from master: " + psyncResponse);
                System.out.println("Finished skipping RDB file.");
            } else {
                System.out.println("Unexpected response to PSYNC: " + psyncResponse);
                return;
            }
            String readVal = reader.readLine();
            int length = Integer.parseInt(readVal.substring(1));
            long skipval = reader.skip(length-1);
            if(skipval != length){
                System.out.println("Unable to skip " + length + " chars");
            }
            else{
                System.out.println("Values Skipped " + skipval);
            }
            System.out.println("read: " + readVal);
            while (true){
                String inputLine = reader.readLine();
                if(inputLine==null) break;

                if(inputLine.startsWith("*")){
                    String[] commandParts = parseMasterRespCommand(reader, inputLine);
                    if(commandParts != null && commandParts.length > 0){
                        String command = commandParts[0].toUpperCase();

                        switch (command){
                            case "PING":
                                int commandSize = calculateCommandSize(commandParts);
                                offset += commandSize;
                                break;
                            case "REPLCONF":
                                handleReplicaReplconfCommand(out, commandParts);
                                break;
                            case "SET":
                                processSetCommands(commandParts);
                                break;
                            default:
                                System.out.println("-ERR unknown command\r\n");
                        }
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("IOException when connecting to master: " + e.getMessage());
        }
    }

    private static void handleReplicaReplconfCommand(OutputStream out, String[] commandParts) throws IOException {
        if (commandParts.length >= 2) {
            String subCommand = commandParts[1].toUpperCase();
            if ("GETACK".equals(subCommand)) {
                System.out.println("GETACK check");
//                ClientHandler.handleReplicaAck(ClientHandler.currentOffset);
                String response = String.format("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", String.valueOf(offset).length(), offset);
                System.out.println("Sent REPLCONF ACK " + offset + " to master");
                out.write(response.getBytes());
                // Calculate the size of the SET command in bytes
                int commandSize = calculateCommandSize(commandParts);
                offset += commandSize; // Update the offset
            } else {
                System.out.println("Unknown REPLCONF subcommand: " + subCommand);
            }
        } else {
            System.out.println("-ERR insufficient arguments for REPLCONF");
        }
    }

    private static void processSetCommands(String[] commandParts) throws IOException {
        if (commandParts.length < 3) {
            System.out.println("-ERR wrong number of arguments for 'SET' command\r\n".getBytes());
            return;
        }

        // Calculate the size of the SET command in bytes
        int commandSize = calculateCommandSize(commandParts);
        offset += commandSize; // Update the offset

        String key = commandParts[1];
        String value = commandParts[2];
        long expiryTime = -1;

        if (commandParts.length >= 5 && commandParts[3].equalsIgnoreCase("PX")) {
            try {
                long expiryInMilliseconds = Long.parseLong(commandParts[4]);
                expiryTime = System.currentTimeMillis() + expiryInMilliseconds;
            } catch (NumberFormatException e) {
                System.out.println("-ERR invalid PX argument\r\n".getBytes());
                return;
            }
        }

        //Check this later
        ClientHandler.KeyValueStore.put(key, new ValueWithExpiry(value, expiryTime));
    }

    private static String[] parseMasterRespCommand(BufferedReader reader, String firstLine) throws IOException {
        int numElements = Integer.parseInt(firstLine.substring(1));
        String[] commandParts = new String[numElements];

        for (int i = 0; i < numElements; i++) {
            String lengthLine = reader.readLine();
            if (lengthLine.startsWith("$")) {
                String bulkString = reader.readLine();
                commandParts[i] = bulkString;
            }
        }

        System.out.println("Replica: Parsed RESP Command: " + String.join(", ", commandParts));
        return commandParts;
    }

    private static int calculateCommandSize(String[] commandParts) {
        int size = 0;
        size += ("*" + commandParts.length + "\r\n").length();
        for (String part : commandParts) {
            size += ("$" + part.length() + "\r\n").length();
            size += (part + "\r\n").length();
        }
        return size;
    }
}
