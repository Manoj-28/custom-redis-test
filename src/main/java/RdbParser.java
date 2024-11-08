import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RdbParser{
    public static void loadRDB(String dir, String dbfFilename){
        File rdbFile = new File(dir,dbfFilename);
        if(!rdbFile.exists()){
            System.out.println("RDB file not found, Treating database as empty.");
            return;
        }

        try(FileInputStream fis = new FileInputStream(rdbFile)) {
            byte[] header = new byte[8];
            int bytesRead = fis.read(header);
            // Check if the correct number of bytes was read
            if (bytesRead != header.length) {
                System.out.println("Error: Could not read the full header.");
                return;  // Handle the error appropriately
            }
            System.out.println("RDB header: " + new String(header));
            while(fis.available() > 0){
                int ignore = fis.read();
                int firstByte = fis.read();
                switch (firstByte){
                    case 0xFE:  //Database Section
                        parseDatabaseSection(fis);
                        break;
                    case 0xFF:
                        System.out.println("End of RDB file");
                        break;
                    default:
                        System.out.println("Unknown byte encountered: " + firstByte);
                        return;
                }
            }

        } catch (Exception e) {
           System.out.println("Error loading RDB file: " + e.getMessage());

        }
    }

    private static void parseDatabaseSection(FileInputStream fis) throws Exception {
        int dbIndex = fis.read();       //Read Datbase Index
        System.out.println("Database Index: " + dbIndex);

         int hashTableSize = decodeSize(fis);
         System.out.println("Hash table size: " + hashTableSize);

         for(int i=0;i<hashTableSize;i++){
             int valueType = fis.read();
             String key = readString(fis);
             String value = readString(fis);

             System.out.println("Parsed key-value: " + key + " -> " + value);
             ClientHandler.KeyValueStore.put(key, new ValueWithExpiry(value,-1));  //store in memory
         }

    }

    private static String readString(FileInputStream fis) throws Exception {
        int size = decodeSize(fis);
        byte[] stringBytes = new byte[size];
        fis.read(stringBytes);
        return new String(stringBytes);
    }

    private static int decodeSize(FileInputStream fis) throws Exception {
        int firstByte = fis.read();
        int size = firstByte & 0x3F;  //last 6 bits
        if((firstByte & 0xC0) == 0x40){
            int secondByte = fis.read();
            size = (size << 8) | secondByte;
        }
        else if((firstByte & 0xC0) == 0x80){
            byte[] sizeBytes = new byte[4];
            fis.read(sizeBytes);
            size = ByteBuffer.wrap(sizeBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        }
        return size;
    }
}