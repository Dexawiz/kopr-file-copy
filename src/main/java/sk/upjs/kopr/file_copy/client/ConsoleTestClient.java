package sk.upjs.kopr.file_copy.client;

import java.io.File;
import java.util.concurrent.CountDownLatch;

public class ConsoleTestClient {
    public static void main(String[] args) throws InterruptedException {
        File target = new File("downloaded_testfile.txt");
        DownloadManager manager = new DownloadManager(target);

        int threads = manager.getDownloadState().hasExistingDownload()
                ? manager.getDownloadState().getSavedThreadsCount()
                : 6;

        System.out.println("Starting downloading using " + threads + " threads...");


        CountDownLatch finishLatch = new CountDownLatch(1);

        manager.start(
                threads,
                (progress, downloaded, total) -> {
                    System.out.printf("Progress: %.1f%% (%.2f / %.2f MB)\r",
                            progress * 100,
                            downloaded / (1024.0 * 1024.0),
                            total / (1024.0 * 1024.0));
                },
                () -> {
                    System.out.println("\nDownloaded successfully!");
                    finishLatch.countDown();
                },
                error -> {
                    System.err.println("\nError: " + error);
                    finishLatch.countDown();
                }
        );

        // waiting signal
        try {
            finishLatch.await();
        } finally {
            manager.shutdown(() -> {});
        }
    }
}