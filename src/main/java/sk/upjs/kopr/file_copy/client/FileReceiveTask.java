package sk.upjs.kopr.file_copy.client;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import sk.upjs.kopr.file_copy.FileRequest;

public class FileReceiveTask implements Callable<Void> {

    private static final int BUFFER_SIZE = 65536;

    private final MyFileWriter myFileWriter;
    private final long fileSize;
    private final InetAddress inetAddress;
    private final int serverPort;
    private final Queue<Integer> chunkQueue;
    private final DownloadState downloadState;
    private final CountDownLatch latch;
    private final AtomicLong totalDownloadedBytes;

    private final DownloadManager.DownloadSession session;

    public FileReceiveTask(MyFileWriter myFileWriter, long fileSize, InetAddress inetAddress, int serverPort, Queue<Integer> chunkQueue, DownloadState downloadState, CountDownLatch latch, AtomicLong totalDownloadedBytes, DownloadManager.DownloadSession session) {
        this.myFileWriter = myFileWriter;
        this.fileSize = fileSize;
        this.inetAddress = inetAddress;
        this.serverPort = serverPort;
        this.chunkQueue = chunkQueue;
        this.downloadState = downloadState;
        this.latch = latch;
        this.totalDownloadedBytes = totalDownloadedBytes;
        this.session = session;
    }

    @Override
    public Void call() {

        try (Socket socket = session.openSocket()) {

            try {
                socket.connect(new InetSocketAddress(inetAddress, serverPort), 5000);

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                oos.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                oos.writeUTF("file");
                oos.flush();

                byte[] buffer = new byte[BUFFER_SIZE];

                while (!session.isStopped()) {

                    checkInterrupted();

                    Integer chunk = chunkQueue.poll();

                    if (chunk == null) {
                        oos.writeObject(new FileRequest(-1, -1));
                        oos.flush();
                        break;
                    }

                    long offset = (long) chunk * DownloadState.CHUNK_SIZE;

                    long length = Math.min(DownloadState.CHUNK_SIZE, fileSize - offset);
                    long bytesReadForChunk = downloadState.getChunkDownloadedBytes(chunk);
                    if (bytesReadForChunk > length) {
                        throw new IOException("Rozpracovaný súbor má neplatný stav.");
                    }

                    oos.writeObject(new FileRequest(offset + bytesReadForChunk, length - bytesReadForChunk));

                    oos.flush();

                    while (bytesReadForChunk < length) {

                        checkInterrupted();

                        if (session.isStopped()) {
                            return null;
                        }

                        int toRead = (int) Math.min(BUFFER_SIZE, length - bytesReadForChunk);

                        int read = ois.read(buffer, 0, toRead);

                        if (read == -1) {
                            throw new EOFException("Server closed connection");
                        }

                        myFileWriter.write(offset + bytesReadForChunk, buffer, 0, read);

                        bytesReadForChunk += read;
                        downloadState.markChunkProgress(chunk, bytesReadForChunk);
                        totalDownloadedBytes.addAndGet(read);
                    }
                }

            } finally {
                session.removeSocket(socket);
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            session.fail("Sťahovanie bolo prerušené.");

        } catch (IOException e) {

            if (!session.isStopped()) {
                session.fail("Sťahovanie bolo prerušené: " + e.getMessage());
            }

        } finally {
            latch.countDown();
        }

        return null;
    }

    private void checkInterrupted() throws InterruptedException {

        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }
}