import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RdbParser {
    public static void loadRDB(String dir, String dbfFilename) {
        File rdbFile = new File(dir, dbfFilename);
        if (!rdbFile.exists()) {
            System.out.println("RDB file not found, Treating database as empty.");
            return;
        }

        try (FileInputStream fis = new FileInputStream(rdbFile)) {
            byte[] header = new byte[8];
            int bytesRead = fis.read(header);
            if (bytesRead != header.length) {
                System.out.println("Error: Could not read the full header.");
                return;
            }
            System.out.println("RDB header: " + new String(header));

            while (fis.available() > 0) {
                int ignore = fis.read();
                int marker = fis.read();
//                while((marker = fis.read()) != 0xFE);
                switch (marker) {
                    case 0xFA:  // Metadata Section
                        parseMetadataSection(fis);
                        break;
                    case 0xFE:  // Database Section
                        parseDatabaseSection(fis);
                        break;
                    case 0xFF:  // End of File
                        System.out.println("End of RDB file");
                        byte[] checksum = new byte[8];
                        int checkSumRead = fis.read(checksum,0,checksum.length);
                        System.out.println("Checksum: " + ByteBuffer.wrap(checksum).getLong());
                        break;
                    default:
                        System.out.println("Unknown marker encountered: " + marker);
                        return;
                }
            }
        } catch (Exception e) {
            System.out.println("Error loading RDB file: " + e.getMessage());
        }
    }

    private static void parseMetadataSection(FileInputStream fis) throws Exception {
        while (fis.available() > 0) {
            int marker = fis.read();
            if (marker != 0xFA) break;

            String attributeName = readString(fis);
            String attributeValue = readString(fis);

            System.out.println("Metadata: " + attributeName + " = " + attributeValue);
        }
    }

    private static void parseDatabaseSection(FileInputStream fis) throws Exception {
        int dbIndex = fis.read();
        System.out.println("Database Index: " + dbIndex);

        int hashTableSize = decodeSize(fis);
        System.out.println("Hash table size: " + hashTableSize);

        int encryptedKeys = fis.read();
        int keyType = fis.read();

        for (int i = 0; i < hashTableSize; i++) {
//            int keySize = fis.read();
//            byte[] keyBytes = new byte[keySize];
//            int ks = fis.read(keyBytes);
//            String key = new String(keyBytes);
            String key = readString(fis);
//            int valueSize = fis.read();
//            byte[] valueBytes = new byte[valueSize];
//            int vs = fis.read(valueBytes);
//            String value = new String(valueBytes);
            String value = readString(fis);

            System.out.println("Parsed key-value: " + key + " -> " + value);
            ClientHandler.KeyValueStore.put(key, new ValueWithExpiry(value, -1));
        }
    }

    private static String readString(FileInputStream fis) throws Exception {
//        int size = decodeSize(fis);
        int size = fis.read();
        byte[] stringBuffer = new byte[size];
         int stringBytes = fis.read(stringBuffer);
        return new String(stringBuffer);
    }

    private static int decodeSize(FileInputStream fis) throws Exception {
        int sizeInfo = fis.read();
        int firstByte = fis.read();
        int size = firstByte & 0x3F;
        if ((firstByte & 0xC0) == 0x40) {
            int secondByte = fis.read();
            size = (size << 8) | secondByte;
        } else if ((firstByte & 0xC0) == 0x80) {
            byte[] sizeBuffer = new byte[4];
            int sizeBytes = fis.read(sizeBuffer);
            size = ByteBuffer.wrap(sizeBuffer).order(ByteOrder.BIG_ENDIAN).getInt();
        }
        return size;
    }
}
