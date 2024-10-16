import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

class ClientHandler extends Thread {
    private Socket clientSocket;

    public ClientHandler(Socket socket){
        this.clientSocket = socket;
    }

    @Override
    public void run(){
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream out = clientSocket.getOutputStream();

            String inputLine;

            while((inputLine = reader.readLine()) != null){
                if(inputLine.equals("PING")){
                    out.write("+PONG\r\n".getBytes());
                }
            }
        }
        catch (IOException e){
            System.out.println("IOException in client Handler: " + e.getMessage());
        } finally{
            try{
                if(clientSocket != null){
                    clientSocket.close();
                }
            }
            catch(IOException e){
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}

public class Main {
    public static void main(String[] args) {
        int port = 6379;

        try (ServerSocket serverSocket = new ServerSocket(port)){
            serverSocket.setReuseAddress(true);

            while(true){
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                // Create a new thread to handle the client
                clientHandler.start();  // Start the thread for this client
            }


        } catch(IOException e){
            System.out.println("IOException: " + e.getMessage());
        }
    }
}