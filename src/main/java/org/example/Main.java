package org.example;

import org.example.model.*;
import org.example.service.FileIndexerService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    private static final int WORKER_COUNT = 3;
    private static final int MAX_RECOVERY_ATTEMPTS = 3;
    private final static BlockingQueue<MainEvent> events = new LinkedBlockingQueue<>();
    private final static Thread consoleLineThread = new Thread(() -> {
        Scanner scanner = new Scanner(System.in);

        try {
            while (scanner.hasNextLine()) {
                events.put(new ConsoleLineEvent(scanner.nextLine()));
            }
            events.put(new ConsoleClosableInvent());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

    });


    public static void main(String[] args) {

        try {
            long countAttempt = 0;
            consoleLineThread.setDaemon(true);
            consoleLineThread.start();

            while (true) {
                System.out.println("Введите путь к папке:");

                var mainResult = events.take();

                if (mainResult instanceof ConsoleClosableInvent) {
                    consoleLineThread.interrupt();
                    consoleLineThread.join();
                    return;
                }

                if (!(mainResult instanceof ConsoleLineEvent(String line))) {
                    continue;
                }

                if (line.isBlank()) {
                    continue;
                }

                Path root;

                try {
                    root = Path.of(line)
                            .toAbsolutePath()
                            .normalize();
                } catch (InvalidPathException e) {
                    System.out.println(e.getMessage());
                    continue;
                }



                if (!Files.isDirectory(root)) {
                    System.out.println("Папка не найдена");
                    continue;
                }

                FileIndexerService indexerService = new FileIndexerService(
                        WORKER_COUNT,
                        root,
                        MAX_RECOVERY_ATTEMPTS
                );

                countAttempt++;
                try {
                    long currentRunId = countAttempt;
                    indexerService.getTermination()
                            .whenComplete((terminationResult, throwable) -> {
                                events.offer(new ServiceTerminatedEvent(currentRunId, throwable));
                            });

                    indexerService.start();

                    FileIndexerSnapshot snapshot = indexerService.snapshot();

                    printState(snapshot);

                    while (true) {
                        MainEvent event = events.take();

                        if (event instanceof ServiceTerminatedEvent serviceTerminatedEvent) {
                            if (currentRunId == serviceTerminatedEvent.runId()) {
                                if (serviceTerminatedEvent.cause() == null) {
                                    System.out.println("Сервис завершена корректно ");
                                } else {
                                    System.out.println("Сервис завершена с ошибкой " + serviceTerminatedEvent.cause());
                                }
                                break;
                            }
                        }
                        if (event instanceof ConsoleClosableInvent) {
                            indexerService.shutdown();
                            return;
                        }
                        if (event instanceof ConsoleLineEvent consoleLineEvent) {
                            indexerService.shutdown();
                            break;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                } finally {
                    indexerService.shutdown();

                    FileIndexerSnapshot finalSnapshot =
                            indexerService.snapshot();

                    System.out.println("Итоговое состояние:");
                    printState(finalSnapshot);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
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

