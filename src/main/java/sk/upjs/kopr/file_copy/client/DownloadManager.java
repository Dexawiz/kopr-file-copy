package sk.upjs.kopr.file_copy.client;

import sk.upjs.kopr.file_copy.FileInfo;
import sk.upjs.kopr.file_copy.server.Server;

import java.io.*;
import java.net.*;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DownloadManager {

    private final File targetFile;
    private final DownloadState downloadState;
    private static final String SERVER_HOST = "localhost";

    private final ExecutorService managerExecutor = Executors.newSingleThreadExecutor();

    private DownloadSession activeSession;

    public DownloadManager(File targetFile) {
        this.targetFile = targetFile;
        this.downloadState = new DownloadState(targetFile);
    }

    public DownloadState getDownloadState() {
        return downloadState;
    }

    public synchronized void start(int numThreads, ProgressConsumer onProgressUpdate, Runnable onFinished, Consumer<String> onError) {

        if (activeSession != null || managerExecutor.isShutdown()) {
            return;
        }

        if (downloadState.hasExistingDownload()) {
            numThreads = downloadState.getSavedThreadsCount();
        }

        if (numThreads < 1 || numThreads > 16) {
            onError.accept("Neplatný počet vlákien.");
            return;
        }

        DownloadSession session = new DownloadSession();
        activeSession = session;

        final int threadCount = numThreads;

        managerExecutor.submit(() -> {

            String error = null;

            try {
                download(threadCount, session, onProgressUpdate);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                error = "Sťahovanie bolo prerušené.";

            } catch (Exception e) {

                error = "Chyba pri sťahovaní: " + e.getMessage();

            } finally {

                session.stop();

                synchronized (this) {
                    activeSession = null;
                }
            }

            if (session.isCancelled() && session.getError() == null) {
                return;
            }

            if (error == null) {
                onFinished.run();
            } else {
                onError.accept(error);
            }
        });
    }

    private void download(int numThreads, DownloadSession session, ProgressConsumer onProgressUpdate) throws IOException, InterruptedException, ClassNotFoundException {

        // 1. Metadata zo servera

        FileInfo fileInfo = receiveFileInfo(session);

        long fileSize = fileInfo.size;

        if (fileSize < 0) {
            throw new IOException("Neplatná veľkosť súboru.");
        }

        long totalChunks = (fileSize + DownloadState.CHUNK_SIZE - 1) / DownloadState.CHUNK_SIZE;

        // 2. Initialize download state

        if (downloadState.hasExistingDownload()) {

            if (!targetFile.isFile() || targetFile.length() != fileSize) {
                if (downloadState.getAlreadyDownloadedBytes(fileSize) > 0) {
                    throw new IOException("Chýba alebo je poškodený rozpracovaný súbor.");
                }
                downloadState.initNewState(numThreads);
            }

        } else {
            downloadState.initNewState(numThreads);
        }

        // 3. Queue of missing chunks

        Queue<Integer> chunkQueue = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < totalChunks; i++) {

            long chunkLength = Math.min(DownloadState.CHUNK_SIZE, fileSize - (long) i * DownloadState.CHUNK_SIZE);
            if (downloadState.getChunkDownloadedBytes(i) < chunkLength) {
                chunkQueue.add(i);
            }
        }

        AtomicLong totalDownloadedBytes = new AtomicLong(downloadState.getAlreadyDownloadedBytes(fileSize));

        InetAddress inetAddress = InetAddress.getByName(SERVER_HOST);

        downloadState.startTracking();

        // 4. ExecutorService and synchronizer
        ExecutorService workers = Executors.newFixedThreadPool(numThreads);

        ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();

        CountDownLatch latch = new CountDownLatch(numThreads);

        // 5. One shared file writer

        try (MyFileWriter writer = new MyFileWriter(targetFile, fileSize)) {

            try {

                synchronized (session) {

                    if (session.isStopped()) {
                        throw new InterruptedException();
                    }

                    for (int i = 0; i < numThreads; i++) {

                        workers.submit(new FileReceiveTask(writer, fileSize, inetAddress, Server.SERVER_PORT, chunkQueue, downloadState, latch, totalDownloadedBytes, session));
                    }

                    session.attachWorkers(workers, latch);
                }

                // 6. Progress updates

                progressExecutor.scheduleAtFixedRate(() -> {

                    long downloaded = totalDownloadedBytes.get();

                    double progress = fileSize == 0 ? 1.0 : (double) downloaded / fileSize;

                    onProgressUpdate.accept(Math.min(progress, 1.0), downloaded, fileSize);

                }, 0, 1, TimeUnit.SECONDS);

                // 7. Wait for all workers

                latch.await();

            } finally {

                // Closes sockets and interrupts workers
                session.stop();

                workers.shutdown();
                progressExecutor.shutdownNow();

                // Wait before closing the shared file
                awaitTermination(workers);
                awaitTermination(progressExecutor);
            }

        } finally {

            workers.shutdown();
            progressExecutor.shutdownNow();
            downloadState.closeTracking();
        }

        // 8. Check whether every chunk is completed

        if (session.getError() != null) {
            throw new IOException(session.getError());
        }

        for (int i = 0; i < totalChunks; i++) {

            long chunkLength = Math.min(DownloadState.CHUNK_SIZE, fileSize - (long) i * DownloadState.CHUNK_SIZE);
            if (downloadState.getChunkDownloadedBytes(i) < chunkLength) {

                throw new IOException("Sťahovanie bolo prerušené. " + "Môžete pokračovať neskôr.");
            }
        }

        // 9. Successful download

        downloadState.deleteStateFile();

        onProgressUpdate.accept(1.0, fileSize, fileSize);
    }

    private FileInfo receiveFileInfo(DownloadSession session) throws IOException, ClassNotFoundException {

        try (Socket socket = session.openSocket()) {

            try {

                socket.connect(new InetSocketAddress(SERVER_HOST, Server.SERVER_PORT), 5000);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                oos.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                oos.writeUTF("info");
                oos.flush();

                return (FileInfo) ois.readObject();

            } finally {
                session.removeSocket(socket);
            }
        }
    }

    public synchronized void cancel() {

        if (activeSession != null) {
            activeSession.cancel();
        }
    }

    public synchronized void shutdown(Runnable onStopped) {

        cancel();

        if (!managerExecutor.isShutdown()) {

            managerExecutor.submit(onStopped);
            managerExecutor.shutdown();
        }
    }

    private static void awaitTermination(ExecutorService executor) {

        boolean interrupted = false;

        try {

            while (true) {

                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    return;

                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

        } finally {

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // State and resources of one download attempt

    static class DownloadSession {

        private final Set<Socket> sockets = new HashSet<>();

        private volatile boolean stopped;
        private volatile boolean cancelled;

        private String error;

        private ExecutorService workers;
        private CountDownLatch latch;

        public synchronized Socket openSocket() throws IOException {

            if (stopped) {
                throw new IOException("Download stopped");
            }

            Socket socket = new Socket();

            sockets.add(socket);

            return socket;
        }

        public synchronized void removeSocket(Socket socket) {
            sockets.remove(socket);
        }

        public synchronized void attachWorkers(ExecutorService workers, CountDownLatch latch) {
            this.workers = workers;
            this.latch = latch;
        }

        public boolean isStopped() {
            return stopped;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public synchronized String getError() {
            return error;
        }

        public synchronized void fail(String error) {

            if (!stopped) {
                this.error = error;
                stop();
            }
        }

        public synchronized void stop() {

            if (stopped) {
                return;
            }

            stopped = true;

            for (Socket socket : sockets) {

                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Socket is being closed
                }
            }

            sockets.clear();

            if (workers != null) {

                int cancelledTasks = workers.shutdownNow().size();

                // Tasks that never started cannot reach finally
                for (int i = 0; i < cancelledTasks; i++) {
                    latch.countDown();
                }
            }
        }

        public synchronized void cancel() {
            cancelled = true;
            stop();
        }
    }

    @FunctionalInterface
    public interface ProgressConsumer {

        void accept(double progressFraction, long downloadedBytes, long totalBytes);
    }
}