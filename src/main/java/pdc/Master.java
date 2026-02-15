package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cluster coordinator.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, TaskInfo> activeTasks = new ConcurrentHashMap<>();
    private final Queue<TaskInfo> pendingTasks = new ConcurrentLinkedQueue<>();
    private final AtomicInteger taskIdCounter = new AtomicInteger(0);
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private final String studentId;
    private final int port;
    private static final long HEARTBEAT_INTERVAL = 5000;
    private static final long HEARTBEAT_TIMEOUT = 15000;

    public Master(int port) {
        this.port = port;
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "default-student");
    }


    private static class WorkerConnection {
        final String workerId;
        final Socket socket;
        final OutputStream output;
        final InputStream input;
        volatile long lastHeartbeat;
        volatile boolean alive;
        volatile TaskInfo currentTask;

        WorkerConnection(String workerId, Socket socket) throws IOException {
            this.workerId = workerId;
            this.socket = socket;
            this.output = socket.getOutputStream();
            this.input = socket.getInputStream();
            this.lastHeartbeat = System.currentTimeMillis();
            this.alive = true;
        }

        synchronized void sendMessage(Message msg) throws IOException {
            byte[] data = msg.pack();
            output.write(data);
            output.flush();
        }

        void close() {
            try {
                socket.close();
            } catch (IOException e) {}
        }
    }


    private static class TaskInfo {
        final String taskId;
        final String operation;
        final byte[] data;
        volatile String assignedWorker;
        volatile byte[] result;
        volatile boolean completed;
        volatile boolean failed;
        final CountDownLatch latch;

        TaskInfo(String taskId, String operation, byte[] data) {
            this.taskId = taskId;
            this.operation = operation;
            this.data = data;
            this.latch = new CountDownLatch(1);
        }
    }


    public Object coordinate(String operation, int[][] data, int workerCount) {
        try {

            long startWait = System.currentTimeMillis();
            while (workers.size() < workerCount && System.currentTimeMillis() - startWait < 30000) {
                Thread.sleep(100);
            }

            if (workers.isEmpty()) {
                System.err.println("No workers available");
                return null;
            }

            System.out.println("Starting coordination with " + workers.size() + " workers");


            List<MatrixBlock> blocks = partitionMatrix(data, workers.size());
            

            List<TaskInfo> tasks = new ArrayList<>();
            for (MatrixBlock block : blocks) {
                byte[] blockData = serializeMatrixBlock(block);
                String taskId = "task-" + taskIdCounter.incrementAndGet();
                TaskInfo task = new TaskInfo(taskId, operation, blockData);
                tasks.add(task);
                activeTasks.put(taskId, task);
                pendingTasks.offer(task);
            }


            assignTasks();


            for (TaskInfo task : tasks) {
                try {
                    task.latch.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }


            int[][] result = aggregateResults(tasks, data.length, data[0].length);
            

            for (TaskInfo task : tasks) {
                activeTasks.remove(task.taskId);
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    private List<MatrixBlock> partitionMatrix(int[][] matrix, int numBlocks) {
        List<MatrixBlock> blocks = new ArrayList<>();
        int rows = matrix.length;
        int rowsPerBlock = Math.max(1, rows / numBlocks);

        for (int i = 0; i < rows; i += rowsPerBlock) {
            int endRow = Math.min(i + rowsPerBlock, rows);
            int[][] blockData = new int[endRow - i][];
            for (int j = i; j < endRow; j++) {
                blockData[j - i] = matrix[j].clone();
            }
            blocks.add(new MatrixBlock(i, endRow, blockData));
        }

        return blocks;
    }


    private byte[] serializeMatrixBlock(MatrixBlock block) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        dos.writeInt(block.startRow);
        dos.writeInt(block.endRow);
        dos.writeInt(block.data.length);
        dos.writeInt(block.data[0].length);
        
        for (int[] row : block.data) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }
        
        dos.flush();
        return baos.toByteArray();
    }


    private MatrixBlock deserializeMatrixBlock(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
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
        
        return new MatrixBlock(startRow, endRow, matrix);
    }


    private int[][] aggregateResults(List<TaskInfo> tasks, int totalRows, int totalCols) {
        int[][] result = new int[totalRows][totalCols];
        
        for (TaskInfo task : tasks) {
            if (task.result != null) {
                try {
                    MatrixBlock block = deserializeMatrixBlock(task.result);
                    int targetRow = block.startRow;
                    for (int i = 0; i < block.data.length; i++) {
                        System.arraycopy(block.data[i], 0, result[targetRow + i], 0, block.data[i].length);
                    }
                } catch (IOException e) {
                    System.err.println("Failed to deserialize result for " + task.taskId);
                }
            }
        }
        
        return result;
    }


    private void assignTasks() {
        for (WorkerConnection worker : workers.values()) {
            if (worker.alive && worker.currentTask == null) {
                TaskInfo task = pendingTasks.poll();
                if (task != null) {
                    assignTaskToWorker(task, worker);
                }
            }
        }
    }


    private void assignTaskToWorker(TaskInfo task, WorkerConnection worker) {
        try {
            task.assignedWorker = worker.workerId;
            worker.currentTask = task;
            
            Message msg = new Message(Message.RPC_REQUEST, "master", task.data);
            msg.type = Message.RPC_REQUEST;
            msg.sender = "master";
            msg.payload = task.data;
            

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(task.taskId);
            dos.writeUTF(task.operation);
            dos.write(task.data);
            dos.flush();
            
            msg.payload = baos.toByteArray();
            
            worker.sendMessage(msg);
            System.out.println("Assigned " + task.taskId + " to " + worker.workerId);
        } catch (IOException e) {
            System.err.println("Failed to assign task to " + worker.workerId);
            worker.alive = false;
            pendingTasks.offer(task);
        }
    }


    public void listen(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.running = true;
        
        System.out.println("Master listening on port " + port);
        
        systemThreads.submit(this::heartbeatMonitor);
        
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }


    private void handleWorkerConnection(Socket socket) {
        try {
            System.out.println("New connection from " + socket.getRemoteSocketAddress());
            
            Message regMsg = Message.readFromStream(socket.getInputStream());
            
            if (!regMsg.validate() || !Message.REGISTER_WORKER.equals(regMsg.type)) {
                System.err.println("Invalid registration message");
                socket.close();
                return;
            }
            
            String workerId = regMsg.sender;
            WorkerConnection worker = new WorkerConnection(workerId, socket);
            workers.put(workerId, worker);
            
            Message ack = new Message(Message.WORKER_ACK, "master", new byte[0]);
            worker.sendMessage(ack);
            
            System.out.println("Worker registered: " + workerId);
            
            assignTasks();
            
            while (running && worker.alive) {
                try {
                    socket.setSoTimeout(1000);
                    Message msg = Message.readFromStream(socket.getInputStream());
                    handleWorkerMessage(worker, msg);
                } catch (SocketTimeoutException e) {
                } catch (IOException e) {
                    System.err.println("Worker " + workerId + " disconnected");
                    worker.alive = false;
                    break;
                }
            }
            
        } catch (IOException e) {
            System.err.println("Error handling worker connection: " + e.getMessage());
        }
    }


    private void handleWorkerMessage(WorkerConnection worker, Message msg) {
        try {
            switch (msg.type) {
                case Message.HEARTBEAT_ACK:
                    worker.lastHeartbeat = System.currentTimeMillis();
                    break;
                    
                case Message.TASK_COMPLETE:
                case Message.RPC_RESPONSE:
                    handleTaskResult(worker, msg);
                    break;
                    
                case Message.TASK_ERROR:
                    handleTaskError(worker, msg);
                    break;
                    
                default:
                    System.out.println("Received message: " + msg.type + " from " + worker.workerId);
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
        }
    }


    private void handleTaskResult(WorkerConnection worker, Message msg) throws IOException {

        ByteArrayInputStream bais = new ByteArrayInputStream(msg.payload);
        DataInputStream dis = new DataInputStream(bais);
        
        String taskId = dis.readUTF();
        byte[] result = new byte[dis.available()];
        dis.readFully(result);
        
        TaskInfo task = activeTasks.get(taskId);
        if (task != null) {
            task.result = result;
            task.completed = true;
            task.latch.countDown();
            worker.currentTask = null;
            
            System.out.println("Task " + taskId + " completed by " + worker.workerId);
            
            TaskInfo nextTask = pendingTasks.poll();
            if (nextTask != null) {
                assignTaskToWorker(nextTask, worker);
            }
        }
    }


    private void handleTaskError(WorkerConnection worker, Message msg) {
        if (worker.currentTask != null) {
            TaskInfo task = worker.currentTask;
            System.err.println("Task " + task.taskId + " failed on " + worker.workerId + ", reassigning");
            worker.currentTask = null;
            pendingTasks.offer(task);
            assignTasks();
        }
    }


    private void heartbeatMonitor() {
        while (running) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL);
                
                long now = System.currentTimeMillis();
                for (WorkerConnection worker : workers.values()) {
                    if (worker.alive) {
                        // Send heartbeat
                        try {
                            Message heartbeat = new Message(Message.HEARTBEAT, "master", new byte[0]);
                            worker.sendMessage(heartbeat);
                        } catch (IOException e) {
                            worker.alive = false;
                        }
                        
                        if (now - worker.lastHeartbeat > HEARTBEAT_TIMEOUT) {
                            System.err.println("Worker " + worker.workerId + " timed out");
                            worker.alive = false;
                            
                            if (worker.currentTask != null) {
                                TaskInfo task = worker.currentTask;
                                worker.currentTask = null;
                                pendingTasks.offer(task);
                                assignTasks();
                            }
                        }
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void reconcileState() {
        workers.values().removeIf(w -> !w.alive);
        assignTasks();
    }

    public void shutdown() {
        running = false;
        
        for (WorkerConnection worker : workers.values()) {
            try {
                Message shutdown = new Message(Message.SHUTDOWN, "master", new byte[0]);
                worker.sendMessage(shutdown);
                worker.close();
            } catch (IOException e) {}
        }
        
        workers.clear();
        systemThreads.shutdown();
        
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {}
    }


    private static class MatrixBlock {
        final int startRow;
        final int endRow;
        final int[][] data;

        MatrixBlock(int startRow, int endRow, int[][] data) {
            this.startRow = startRow;
            this.endRow = endRow;
            this.data = data;
        }
    }

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"));
            Master master = new Master(port);
            
            Thread listenerThread = new Thread(() -> {
                try {
                    master.listen(port);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            listenerThread.start();
            
            Runtime.getRuntime().addShutdownHook(new Thread(master::shutdown));
            
            listenerThread.join();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
