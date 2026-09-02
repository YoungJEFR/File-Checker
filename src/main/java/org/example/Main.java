package org.example;

import org.example.filescanner.FileScanner;
import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.pipeline.FileProducer;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Main {

    private static final int WORKER_COUNT = 3;
    private static final int QUEUE_CAPACITY = 100;

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

            runIndexer(root, console);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Работа программы была прервана");
        }
    }

    private static void runIndexer(
            Path root,
            Scanner console
    ) throws InterruptedException {

        List<BlockingQueue<FileTask>> queues =
                createQueues(WORKER_COUNT);

        FilesStat filesStat = new FilesStat(
                new AtomicInteger(),
                new AtomicInteger(),
                new LongAdder()
        );

        ConcurrentHashMap<Path, FileInfo> indexMap =
                new ConcurrentHashMap<>();

        FileIndex fileIndex = new FileIndex(indexMap);
        TaskRouter taskRouter = new TaskRouter(queues);
        FileScanner fileScanner = new FileScanner();

        ScheduledExecutorService debounceExecutor =
                Executors.newSingleThreadScheduledExecutor(runnable ->
                        new Thread(runnable, "DebounceWorker")
                );

        ExecutorService rescanExecutor =
                Executors.newSingleThreadExecutor(runnable ->
                        new Thread(runnable, "RescanWorker")
                );

        FileChangeDebounce debounce =
                new FileChangeDebounce(
                        debounceExecutor,
                        taskRouter
                );

        Map<WatchKey, Path> registeredDirectories =
                new ConcurrentHashMap<>();

        WatchRegistrar watchRegistrar =
                new WatchRegistrar(
                        new ConcurrentHashMap<>(
                                registeredDirectories
                        )
                );

        Thread[] workers = startWorkers(
                queues,
                filesStat,
                fileIndex
        );

        FileWatcher fileWatcher = new FileWatcher(
                root,
                debounce,
                taskRouter,
                watchRegistrar,
                rescanExecutor,
                fileScanner
        );

        Thread watcherThread =
                new Thread(fileWatcher, "FileWatcher");

        try {
            runInitialScan(
                    root,
                    taskRouter,
                    fileScanner
            );

            System.out.println();
            System.out.println(
                    "Первоначальный обход директории закончен."
            );

            /*
             * На этом этапе producer закончил добавлять задачи,
             * но workers теоретически ещё могут их обрабатывать.
             *
             * Позже здесь следует добавить Barrier +
             * CountDownLatch.
             */
            printState(indexMap, filesStat);

            watcherThread.start();

            System.out.println();
            System.out.println("Наблюдение за папкой запущено.");
            System.out.println();
            System.out.println("Теперь попробуйте:");
            System.out.println("1. Создать .md файл");
            System.out.println("2. Изменить .md файл");
            System.out.println("3. Удалить .md файл");
            System.out.println();
            System.out.println(
                    "Нажмите ENTER для завершения программы."
            );

            console.nextLine();

        } finally {
            System.out.println();
            System.out.println("Останавливаем FileWatcher...");

            stopWatcher(watcherThread);

            shutdownExecutor(
                    rescanExecutor,
                    "Rescan executor"
            );

            shutdownExecutor(
                    debounceExecutor,
                    "Debounce executor"
            );

            stopWorkers(queues, workers);

            System.out.println();
            System.out.println("Итоговое состояние:");

            printState(indexMap, filesStat);

            System.out.println();
            System.out.println("Программа завершена.");
        }
    }

    private static List<BlockingQueue<FileTask>> createQueues(
            int workerCount
    ) {
        List<BlockingQueue<FileTask>> queues =
                new ArrayList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            queues.add(
                    new ArrayBlockingQueue<>(QUEUE_CAPACITY)
            );
        }

        return queues;
    }

    private static Thread[] startWorkers(
            List<BlockingQueue<FileTask>> queues,
            FilesStat filesStat,
            FileIndex fileIndex
    ) {
        Thread[] workers = new Thread[queues.size()];

        for (int i = 0; i < queues.size(); i++) {
            FileWorker worker = new FileWorker(
                    queues.get(i),
                    filesStat,
                    fileIndex
            );

            workers[i] = new Thread(
                    worker,
                    "Worker-" + (i + 1)
            );

            workers[i].start();
        }

        return workers;
    }

    private static void runInitialScan(
            Path root,
            TaskRouter taskRouter,
            FileScanner fileScanner
    ) throws InterruptedException {

        FileProducer producer = new FileProducer(
                root,
                taskRouter,
                fileScanner
        );

        Thread producerThread =
                new Thread(producer, "Producer");

        producerThread.start();
        producerThread.join();
    }

    private static void stopWatcher(
            Thread watcherThread
    ) throws InterruptedException {

        if (!watcherThread.isAlive()) {
            return;
        }

        watcherThread.interrupt();
        watcherThread.join();
    }

    private static void shutdownExecutor(
            ExecutorService executor,
            String executorName
    ) throws InterruptedException {

        executor.shutdown();

        if (executor.awaitTermination(30, TimeUnit.SECONDS)) {
            return;
        }

        System.err.println(
                executorName
                        + " не завершился вовремя. Выполняется shutdownNow()."
        );

        executor.shutdownNow();

        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            System.err.println(
                    executorName + " не удалось завершить"
            );
        }
    }

    private static void stopWorkers(
            List<BlockingQueue<FileTask>> queues,
            Thread[] workers
    ) throws InterruptedException {

        for (BlockingQueue<FileTask> queue : queues) {
            /*
             * Временный вариант.
             * Позже заменить на отдельную Stop-задачу.
             */
            queue.put(
                    new FileTask(
                            Path.of("STOP"),
                            null
                    )
            );
        }

        for (Thread worker : workers) {
            worker.join();
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