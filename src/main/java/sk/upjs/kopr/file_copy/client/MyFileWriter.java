package sk.upjs.kopr.file_copy.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class MyFileWriter implements AutoCloseable {

    private final RandomAccessFile raf;

    public MyFileWriter(File file, long fileSize) throws IOException {

        raf = new RandomAccessFile(file, "rw");

        try {
            raf.setLength(fileSize);
        } catch (IOException e) {
            raf.close();
            throw e;
        }
    }

    public synchronized void write(long fileOffset, byte[] data, int dataOffset, int dataLength) throws IOException {

        raf.seek(fileOffset);
        raf.write(data, dataOffset, dataLength);
    }

    @Override
    public synchronized void close() throws IOException {
        raf.close();
    }
}