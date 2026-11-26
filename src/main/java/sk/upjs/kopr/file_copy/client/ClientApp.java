package sk.upjs.kopr.file_copy.client;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class ClientApp extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception {

        FXMLLoader loader = new FXMLLoader(
                getClass().getResource("/kopr_file.fxml")
        );

        Parent root = loader.load();

        ClientController controller = loader.getController();

        primaryStage.setTitle("KOPR File Downloader");

        primaryStage.setScene(new Scene(root));

        primaryStage.setResizable(false);

        primaryStage.setOnCloseRequest(event -> {

            event.consume();

            controller.shutdown(() ->
                    Platform.runLater(Platform::exit)
            );
        });

        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}