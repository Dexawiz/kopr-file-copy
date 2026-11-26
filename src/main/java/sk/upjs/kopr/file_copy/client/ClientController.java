package sk.upjs.kopr.file_copy.client;

import java.io.File;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;

public class ClientController {

    private static final String TARGET_FILE_NAME = "downloaded_testfile.txt";

    @FXML
    private Spinner<Integer> threadsSpinner;

    @FXML
    private ProgressBar progressBar;

    @FXML
    private Button startButton;

    @FXML
    private Label statusLabel;

    private DownloadManager downloadManager;

    @FXML
    public void initialize() {
        threadsSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 16, 4));

        File targetFile = new File(TARGET_FILE_NAME);
        downloadManager = new DownloadManager(targetFile);

        DownloadState state = downloadManager.getDownloadState();
        if (state.hasExistingDownload()) {
            int savedThreads = state.getSavedThreadsCount();
            threadsSpinner.getValueFactory().setValue(savedThreads);
            threadsSpinner.setDisable(true);
            startButton.setText("Pokračovať v sťahovaní");
            statusLabel.setText("Nájdené prerušené sťahovanie (" + savedThreads + " vlákien)");
        }
    }

    @FXML
    private void handleStart() {
        startButton.setDisable(true);
        threadsSpinner.setDisable(true);
        statusLabel.setText("Pripájam sa na server...");

        int threads = threadsSpinner.getValue();

        downloadManager.start(
                threads,
                (progress, downloaded, total) -> Platform.runLater(() -> {
                    progressBar.setProgress(progress);
                    statusLabel.setText(String.format("Stiahnuté: %.1f %% (%.2f / %.2f MB)",
                            progress * 100,
                            downloaded / (1024.0 * 1024.0),
                            total / (1024.0 * 1024.0)));
                }),
                () -> Platform.runLater(() -> {
                    progressBar.setProgress(1.0);
                    statusLabel.setText("Sťahovanie úspešne dokončené!");
                    startButton.setText("Hotovo");
                    new Alert(Alert.AlertType.INFORMATION, "Súbor bol úspešne stiahnutý!").showAndWait();
                }),
                error -> Platform.runLater(() -> {
                    statusLabel.setText("Chyba: " + error);
                    startButton.setDisable(false);

                    if (downloadManager.getDownloadState().hasExistingDownload()) {
                        startButton.setText("Pokračovať v sťahovaní");
                        threadsSpinner.setDisable(true);
                    } else {
                        startButton.setText("Spustiť sťahovanie");
                        threadsSpinner.setDisable(false);
                    }

                    new Alert(Alert.AlertType.ERROR, error).showAndWait();
                })
        );
    }

    public void shutdown(Runnable onStopped) {
        if (downloadManager == null) {
            onStopped.run();
        } else {
            downloadManager.shutdown(onStopped);
        }
    }
}