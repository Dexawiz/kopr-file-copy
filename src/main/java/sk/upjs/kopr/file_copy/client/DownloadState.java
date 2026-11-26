package sk.upjs.kopr.file_copy.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.Files.deleteIfExists;

public class DownloadState {
    public static final int CHUNK_SIZE = 2 * 1024 * 1024;

    private final File stateFile;
    private final Map<Integer, Long> chunkProgress = new HashMap<>();
    private int savedThreadsCount = -1;
    private long validFileLength;
    private RandomAccessFile stateWriter;

    public DownloadState(File targetFile) {
        this.stateFile = new File(targetFile.getAbsolutePath() + ".state");
        loadState();
    }

    private synchronized void loadState() {
        if (!stateFile.isFile()) {
            return;
        }

        try (RandomAccessFile input = new RandomAccessFile(stateFile, "r")) {
            if (input.length() < Integer.BYTES) {
                return;
            }

            savedThreadsCount = input.readInt();
            if (savedThreadsCount < 1 || savedThreadsCount > 16) {
                resetLoadedState();
                return;
            }

            validFileLength = input.getFilePointer();
            while (input.length() - input.getFilePointer() >= Integer.BYTES + Long.BYTES) {
                int chunkIndex = input.readInt();
                long bytesDownloaded = input.readLong();
                if (chunkIndex >= 0 && bytesDownloaded >= 0) {
                    chunkProgress.put(chunkIndex, bytesDownloaded);
                }
                validFileLength = input.getFilePointer();
            }
        } catch (IOException e) {
            resetLoadedState();
        }
    }

    private void resetLoadedState() {
        chunkProgress.clear();
        savedThreadsCount = -1;
        validFileLength = 0;
    }

    public synchronized void initNewState(int threadsCount) throws IOException {
        closeTracking();
        try (RandomAccessFile output = new RandomAccessFile(stateFile, "rw")) {
            output.setLength(0);
            output.writeInt(threadsCount);
            validFileLength = output.getFilePointer();
        }
        chunkProgress.clear();
        savedThreadsCount = threadsCount;
    }

    public synchronized void startTracking() throws IOException {
        closeTracking();
        stateWriter = new RandomAccessFile(stateFile, "rw");
        stateWriter.setLength(validFileLength);
        stateWriter.seek(validFileLength);
    }

    public synchronized void markChunkProgress(int chunkIndex, long bytesDownloaded) throws IOException {
        if (stateWriter == null) {
            throw new IOException("Download state is not open for updates.");
        }
        if (chunkIndex < 0 || bytesDownloaded < 0) {
            throw new IllegalArgumentException("Invalid chunk progress.");
        }

        long previous = chunkProgress.getOrDefault(chunkIndex, 0L);
        if (bytesDownloaded <= previous) {
            return;
        }

        stateWriter.writeInt(chunkIndex);
        stateWriter.writeLong(bytesDownloaded);
        validFileLength = stateWriter.getFilePointer();
        chunkProgress.put(chunkIndex, bytesDownloaded);
    }

    public synchronized long getChunkDownloadedBytes(int chunkIndex) {
        return chunkProgress.getOrDefault(chunkIndex, 0L);
    }

    public synchronized boolean isChunkCompleted(int chunkIndex) {
        return getChunkDownloadedBytes(chunkIndex) >= CHUNK_SIZE;
    }

    public synchronized long getAlreadyDownloadedBytes(long totalFileSize) {
        long totalChunks = (totalFileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;
        long bytes = 0;
        for (Map.Entry<Integer, Long> entry : chunkProgress.entrySet()) {
            int chunkIndex = entry.getKey();
            if (chunkIndex < 0 || chunkIndex >= totalChunks) {
                continue;
            }
            long chunkLength = Math.min(CHUNK_SIZE, totalFileSize - (long) chunkIndex * CHUNK_SIZE);
            bytes += Math.min(entry.getValue(), chunkLength);
        }
        return bytes;
    }

    public synchronized void deleteStateFile() throws IOException {
        closeTracking();
        deleteIfExists(stateFile.toPath());
        chunkProgress.clear();
        savedThreadsCount = -1;
        validFileLength = 0;
    }

    public synchronized void closeTracking() throws IOException {
        if (stateWriter != null) {
            stateWriter.close();
            stateWriter = null;
        }
    }

    public synchronized boolean hasExistingDownload() {
        return stateFile.isFile() && savedThreadsCount > 0;
    }

    public synchronized int getSavedThreadsCount() {
        return savedThreadsCount;
    }
}