package org.example;

import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.pipeline.FileProducer;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Main {

     static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
         List<BlockingQueue<FileTask>> queues = new ArrayList<>();

        System.out.println("Введите путь к папке:");
        Path root = Path.of(scanner.nextLine());

        if (!Files.exists(root) || !Files.isDirectory(root)) {
            System.out.println("Папка не найдена");
            return;
        }

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger countFiles = new AtomicInteger(0);
        AtomicInteger errorFiles = new AtomicInteger(0);
        LongAdder countByteFiles = new LongAdder();

        FilesStat filesStat =
                new FilesStat(countFiles, errorFiles, countByteFiles);

        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();

        int workerCount = 3;

        for (int i = 0; i < workerCount; i++) {
            queues.add(new ArrayBlockingQueue<>(100));
        }

        // =========================
        // WORKERS
        // =========================

        Thread[] workers = new Thread[workerCount];

        FileIndex fileIndex = new  FileIndex(indexMap);


        for (int i = 0; i < workerCount; i++) {

            FileWorker worker =
                    new FileWorker(
                            queues.get(i),
                            filesStat,
                            fileIndex
                    );

            workers[i] =
                    new Thread(worker, "Worker-" + (i + 1));

            workers[i].start();
        }

        TaskRouter taskRouter = new TaskRouter(queues);
         FileChangeDebounce debounce =
                 new FileChangeDebounce(scheduledExecutorService, taskRouter);
        // =========================
        // INITIAL SCAN
        // =========================

        FileProducer producer =
                new FileProducer(root, taskRouter);

        Thread producerThread =
                new Thread(producer, "Producer");

        producerThread.start();

        try {
            // Ждём завершения первоначального сканирования
            producerThread.join();

            /*
             * ВАЖНО:
             * здесь STOP пока НЕ отправляем.
             *
             * Workers должны продолжать работать,
             * потому что FileWatcher будет отправлять
             * им новые FileTask.
             */

            System.out.println();
            System.out.println("Первоначальное сканирование закончено.");

            printState(indexMap, filesStat);

            // =========================
            // WATCH SERVICE
            // =========================

            FileWatcher fileWatcher =
                    new FileWatcher(root, debounce, taskRouter);

            Thread watcherThread =
                    new Thread(fileWatcher, "FileWatcher");

            watcherThread.start();

            System.out.println();
            System.out.println("Наблюдение за папкой запущено.");
            System.out.println();
            System.out.println("Теперь попробуй:");
            System.out.println("1. Создать .md файл");
            System.out.println("2. Изменить .md файл");
            System.out.println("3. Удалить .md файл");
            System.out.println();
            System.out.println("Нажми ENTER для завершения программы.");

            // Main блокируется здесь,
            // но Watcher и Workers продолжают работать
            scanner.nextLine();

            // =========================
            // SHUTDOWN WATCHER
            // =========================

            System.out.println();
            System.out.println("Останавливаем FileWatcher...");

            watcherThread.interrupt();
            watcherThread.join();
            // =========================

            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
            // =========================
            // SHUTDOWN WORKERS
            // =========================

            /*
             * Один STOP на каждого Worker.
             *
             * null допустим временно,
             * потому что Worker проверяет path == STOP
             * раньше, чем обращается к changeType().
             */
            for (int i = 0; i < workerCount; i++) {
                BlockingQueue<FileTask> filesQueue = queues.get(i);
                filesQueue.put(
                        new FileTask(
                        Path.of("STOP"),
                        null
                ));
            }

            for (Thread worker : workers) {
                worker.join();
            }

            // =========================
            // FINAL RESULT
            // =========================

            System.out.println();
            System.out.println("Итоговое состояние:");

            printState(indexMap, filesStat);

            System.out.println();
            System.out.println("Программа завершена.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            scanner.close();
        }
    }

    private static void printState(
            ConcurrentHashMap<Path, FileInfo> indexMap,
            FilesStat filesStat
    ) {

        System.out.println();
        System.out.println("===== СТАТИСТИКА =====");

        System.out.println(
                "Количество файлов: "
                        + filesStat.getCountFiles()
        );

        System.out.println(
                "Количество байт: "
                        + filesStat.getCountByteFiles()
        );

        System.out.println(
                "Количество ошибок: "
                        + filesStat.getErrorFiles()
        );

        System.out.println();
        System.out.println("===== ИНДЕКС =====");

        indexMap.forEach((path, fileInfo) ->
                System.out.println(
                        path + " -> " + fileInfo
                )
        );

        System.out.println("=====================");
    }
}