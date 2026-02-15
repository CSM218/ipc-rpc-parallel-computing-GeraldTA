package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Message protocol implementation.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;
    
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;
    
    public String messageType;
    public String studentId;

    // Message Types
    public static final String CONNECT = "CONNECT";
    public static final String REGISTER_WORKER = "REGISTER_WORKER";
    public static final String REGISTER_CAPABILITIES = "REGISTER_CAPABILITIES";
    public static final String RPC_REQUEST = "RPC_REQUEST";
    public static final String RPC_RESPONSE = "RPC_RESPONSE";
    public static final String TASK_COMPLETE = "TASK_COMPLETE";
    public static final String TASK_ERROR = "TASK_ERROR";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String HEARTBEAT_ACK = "HEARTBEAT_ACK";
    public static final String WORKER_ACK = "WORKER_ACK";
    public static final String SHUTDOWN = "SHUTDOWN";

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String type, String sender, byte[] payload) {
        this();
        this.type = type;
        this.messageType = type; // Keep in sync
        this.sender = sender;
        this.studentId = sender; // Keep in sync
        this.payload = payload != null ? payload : new byte[0];
    }

    /**
     * Serialize message to bytes.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Write magic string (length + data)
            writeString(dos, magic != null ? magic : MAGIC);
            
            // Write version
            dos.writeInt(version);
            
            // Write type
            writeString(dos, type != null ? type : "");
            
            // Write sender
            writeString(dos, sender != null ? sender : "");
            
            // Write timestamp
            dos.writeLong(timestamp);
            
            // Write payload (length + data)
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }
            
            dos.flush();
            byte[] messageBytes = baos.toByteArray();
            
            // Create final packet with total length prefix
            ByteArrayOutputStream finalBaos = new ByteArrayOutputStream();
            DataOutputStream finalDos = new DataOutputStream(finalBaos);
            finalDos.writeInt(messageBytes.length);
            finalDos.write(messageBytes);
            finalDos.flush();
            
            return finalBaos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Deserialize message from bytes.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            Message msg = new Message();
            
            // Read magic
            msg.magic = readString(dis);
            
            // Read version
            msg.version = dis.readInt();
            
            // Read type
            msg.type = readString(dis);
            
            // Read sender
            msg.sender = readString(dis);
            
            // Read timestamp
            msg.timestamp = dis.readLong();
            
            // Read payload
            int payloadLength = dis.readInt();
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                dis.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }
            
            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }


    public static Message readFromStream(InputStream inputStream) throws IOException {
        DataInputStream dis = new DataInputStream(inputStream);
        
        // Read message length
        int messageLength = dis.readInt();
        
        // Read message data
        byte[] messageData = new byte[messageLength];
        dis.readFully(messageData);
        
        // Unpack message
        return unpack(messageData);
    }


    public boolean validate() {
        if (magic == null || !magic.equals(MAGIC)) {
            return false;
        }
        if (version != VERSION) {
            return false;
        }
        if (type == null || type.isEmpty()) {
            return false;
        }
        return true;
    }

    private static void writeString(DataOutputStream dos, String str) throws IOException {
        if (str == null) {
            dos.writeInt(0);
        } else {
            byte[] bytes = str.getBytes("UTF-8");
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, "UTF-8");
    }

    @Override
    public String toString() {
        return String.format("Message{type='%s', sender='%s', timestamp=%d, payloadSize=%d}",
                type, sender, timestamp, payload != null ? payload.length : 0);
    }
}
