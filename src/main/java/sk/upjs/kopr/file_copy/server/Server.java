package sk.upjs.kopr.file_copy.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {

    public static final int SERVER_PORT = 5000;
    public static final File FILE_TO_SHARE = new File("src/main/resources/testfile.txt");
    public static final AtomicInteger totalTCPconnections = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        File fileToShare = args.length == 0 ? FILE_TO_SHARE : new File(args[0]);
        if (!fileToShare.isFile() || !fileToShare.canRead()) {
            throw new FileNotFoundException("Cannot read shared file: " + fileToShare.getAbsolutePath());
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            System.out.println("Sharing file " + fileToShare.getAbsolutePath() + " (" + fileToShare.length() / (1024 * 1024) + " MB)");
            System.out.println("Server is running on port " + SERVER_PORT + " ...");

            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                int connectionNumber = totalTCPconnections.incrementAndGet();
                System.out.println("New connection accepted. Total connections: " + connectionNumber);
                executor.submit(new FileSendTask(fileToShare, socket));
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
