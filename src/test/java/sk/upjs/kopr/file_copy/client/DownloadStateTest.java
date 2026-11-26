package sk.upjs.kopr.file_copy.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DownloadStateTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void restoresPartialChunkProgressAndThreadCount() throws Exception {
        File target = temporaryDirectory.resolve("download.bin").toFile();
        DownloadState state = new DownloadState(target);
        state.initNewState(4);
        state.startTracking();
        state.markChunkProgress(0, 100_000);
        state.markChunkProgress(1, DownloadState.CHUNK_SIZE);
        state.markChunkProgress(2, 25);
        state.closeTracking();

        DownloadState restored = new DownloadState(target);

        assertEquals(4, restored.getSavedThreadsCount());
        assertEquals(100_000, restored.getChunkDownloadedBytes(0));
        assertEquals(DownloadState.CHUNK_SIZE, restored.getChunkDownloadedBytes(1));
        assertEquals(25, restored.getChunkDownloadedBytes(2));
        assertEquals(DownloadState.CHUNK_SIZE + 100_025L,
                restored.getAlreadyDownloadedBytes(2L * DownloadState.CHUNK_SIZE + 25));
    }
}
