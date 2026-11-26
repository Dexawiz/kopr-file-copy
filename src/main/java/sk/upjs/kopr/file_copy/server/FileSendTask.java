package sk.upjs.kopr.file_copy.server;

import sk.upjs.kopr.file_copy.FileInfo;
import sk.upjs.kopr.file_copy.FileRequest;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketException;

public class FileSendTask implements Runnable {

    private static final int BUFFER_SIZE = 256 * 1024;
    private final File fileToSend;
    private final Socket socket;

    public FileSendTask(File fileToSend, Socket socket) {
        this.fileToSend = fileToSend;
        this.socket = socket;
    }

    @Override
    public void run() {
        try (Socket client = socket; ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream()); RandomAccessFile file = new RandomAccessFile(fileToSend, "r")) {
            output.flush();
            ObjectInputStream input = new ObjectInputStream(client.getInputStream());
            String command = input.readUTF();

            if ("info".equals(command)) {
                output.writeObject(new FileInfo(fileToSend.getName(), file.length()));
                output.flush();
                return;
            }
            if (!"file".equals(command)) {
                return;
            }

            byte[] buffer = new byte[BUFFER_SIZE];
            while (!Thread.currentThread().isInterrupted()) {
                FileRequest request = (FileRequest) input.readObject();
                if (request.offset == -1 && request.length == -1) {
                    return;
                }
                if (request.offset < 0 || request.length < 0 || request.offset > file.length() || request.length > file.length() - request.offset) {
                    throw new IOException("Invalid file range requested");
                }

                file.seek(request.offset);
                long remaining = request.length;
                while (remaining > 0) {
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    int count = file.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (count < 0) {
                        throw new IOException("Source file changed while being transferred");
                    }
                    output.write(buffer, 0, count);
                    remaining -= count;
                }
                output.flush();
            }
        } catch (SocketException | EOFException ignored) {
            // A client disconnects its socket when cancelling or losing the connection.
        } catch (IOException e) {
            System.err.println("Cannot serve " + fileToSend.getName() + ": " + e.getMessage());
        } catch (ClassNotFoundException | ClassCastException e) {
            System.err.println("Invalid request received from client: " + e.getMessage());
        }
    }
}
