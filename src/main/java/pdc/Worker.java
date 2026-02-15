package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Cluster worker node.
 */
public class Worker {

    private final String workerId;
    private final String masterHost;
    private final int masterPort;
    private final String studentId;
    private Socket socket;
    private OutputStream output;
    private InputStream input;
    private volatile boolean running = false;
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "default-student");
    }


    public void joinCluster(String masterHost, int port) {
        try {
            System.out.println("Worker " + workerId + " connecting to master at " + masterHost + ":" + port);
            
            socket = new Socket(masterHost, port);
            output = socket.getOutputStream();
            input = socket.getInputStream();
            
            Message regMsg = new Message(Message.REGISTER_WORKER, workerId, new byte[0]);
            sendMessage(regMsg);
            
            Message ack = Message.readFromStream(input);
            if (ack.validate() && Message.WORKER_ACK.equals(ack.type)) {
                System.out.println("Worker " + workerId + " successfully registered with master");
                running = true;
            } else {
                System.err.println("Failed to register with master");
                socket.close();
                return;
            }
            
            execute();
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public void execute() {
        System.out.println("Worker " + workerId + " entering execution loop");
        
        while (running) {
            try {
                socket.setSoTimeout(1000);
                Message msg = Message.readFromStream(input);
                handleMessage(msg);
            } catch (SocketTimeoutException e) {
            } catch (IOException e) {
                if (running) {
                    System.err.println("Connection to master lost: " + e.getMessage());
                    running = false;
                }
                break;
            }
        }
        
        cleanup();
    }


    private void handleMessage(Message msg) {
        try {
            switch (msg.type) {
                case Message.HEARTBEAT:
                    handleHeartbeat(msg);
                    break;
                    
                case Message.RPC_REQUEST:
                    handleTaskRequest(msg);
                    break;
                    
                case Message.SHUTDOWN:
                    System.out.println("Received shutdown signal");
                    running = false;
                    break;
                    
                default:
                    System.out.println("Received unknown message type: " + msg.type);
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void handleHeartbeat(Message msg) {
        try {
            Message ack = new Message(Message.HEARTBEAT_ACK, workerId, new byte[0]);
            sendMessage(ack);
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat ACK: " + e.getMessage());
        }
    }

    private void handleTaskRequest(Message msg) {
        taskExecutor.submit(() -> {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(msg.payload);
                DataInputStream dis = new DataInputStream(bais);
                
                String taskId = dis.readUTF();
                String operation = dis.readUTF();
                byte[] taskData = new byte[dis.available()];
                dis.readFully(taskData);
                
                System.out.println("Worker " + workerId + " executing task " + taskId);
                
                byte[] result = executeTask(operation, taskData);
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeUTF(taskId);
                dos.write(result);
                dos.flush();
                
                Message response = new Message(Message.TASK_COMPLETE, workerId, baos.toByteArray());
                sendMessage(response);
                
                System.out.println("Worker " + workerId + " completed task " + taskId);
                
            } catch (Exception e) {
                System.err.println("Task execution failed: " + e.getMessage());
                e.printStackTrace();
                
                try {
                    Message errorMsg = new Message(Message.TASK_ERROR, workerId, new byte[0]);
                    sendMessage(errorMsg);
                } catch (IOException ex) {
                    System.err.println("Failed to send error message: " + ex.getMessage());
                }
            }
        });
    }

    private byte[] executeTask(String operation, byte[] taskData) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(taskData);
        DataInputStream dis = new DataInputStream(bais);
        
        int startRow = dis.readInt();
        int endRow = dis.readInt();
        int rows = dis.readInt();
        int cols = dis.readInt();
        
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = dis.readInt();
            }
        }
        
        int[][] result = processMatrix(operation, matrix);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeInt(startRow);
        dos.writeInt(endRow);
        dos.writeInt(result.length);
        dos.writeInt(result[0].length);
        
        for (int[] row : result) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }
        
        dos.flush();
        return baos.toByteArray();
    }


    private int[][] processMatrix(String operation, int[][] matrix) {
        switch (operation.toUpperCase()) {
            case "BLOCK_MULTIPLY":
            case "MULTIPLY":
                return matrix;
                
            case "TRANSPOSE":
                return transposeMatrix(matrix);
                
            case "SCALE":
                return scaleMatrix(matrix, 2);
                
            case "SUM":
                return matrix;
                
            default:
                return matrix;
        }
    }


    private int[][] transposeMatrix(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] result = new int[cols][rows];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[j][i] = matrix[i][j];
            }
        }
        
        return result;
    }


    private int[][] scaleMatrix(int[][] matrix, int factor) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] result = new int[rows][cols];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[i][j] = matrix[i][j] * factor;
            }
        }
        
        return result;
    }


    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        output.write(data);
        output.flush();
    }


    private void cleanup() {
        System.out.println("Worker " + workerId + " shutting down");
        
        taskExecutor.shutdown();
        
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {}
    }

    public static void main(String[] args) {
        String workerId = System.getenv().getOrDefault("WORKER_ID", "worker-" + System.currentTimeMillis());
        String masterHost = System.getenv().getOrDefault("MASTER_HOST", "localhost");
        int masterPort = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"));
        
        Worker worker = new Worker(workerId, masterHost, masterPort);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.running = false;
            worker.cleanup();
        }));
        

        worker.joinCluster(masterHost, masterPort);
    }
}
