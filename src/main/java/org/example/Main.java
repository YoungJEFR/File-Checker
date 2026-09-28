package org.example;

import org.example.model.FileIndexerSnapshot;
import org.example.service.FileIndexerService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

public class Main {

    private static final int WORKER_COUNT = 3;
    private static final int MAX_RECOVERY_ATTEMPTS = 3;

    public static void main(String[] args) {
        try (Scanner console = new Scanner(System.in)) {
            System.out.println("Введите путь к папке:");

            Path root = Path.of(console.nextLine())
                    .toAbsolutePath()
                    .normalize();

            if (!Files.isDirectory(root)) {
                System.out.println("Папка не найдена");
                return;
            }

            FileIndexerService indexerService = new FileIndexerService(
                    WORKER_COUNT,
                    root,
                    MAX_RECOVERY_ATTEMPTS
            );

            try {
                indexerService.start();

                FileIndexerSnapshot snapshot = indexerService.snapshot();

                printState(snapshot);

                System.out.println("---- Нажмите Enter для завершения ----");
                console.nextLine();
            } finally {
                indexerService.shutdown();

                FileIndexerSnapshot finalSnapshot =
                        indexerService.snapshot();

                System.out.println("Итоговое состояние:");
                printState(finalSnapshot);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Поток прервали");
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printState(
            FileIndexerSnapshot snapshot
    ) {
        System.out.println();
        System.out.println("===== СТАТИСТИКА =====");

        System.out.println(
                "Количество файлов: "
                        + snapshot.countFiles()
        );

        System.out.println(
                "Количество байт: "
                        + snapshot.totalBytes()
        );

        System.out.println(
                "Количество ошибок: "
                        + snapshot.errorCount()
        );

        System.out.println();
        System.out.println("===== ИНДЕКС =====");

        snapshot.files().forEach((path, fileInfo) ->
                System.out.println(
                        path + " -> " + fileInfo
                )
        );

        System.out.println("=====================");
    }

}

