package org.example.service;

import org.example.filescanner.FileScanner;
import org.example.model.*;
import org.example.pipeline.FileProducer;
import org.example.pipeline.FileWorker;
import org.example.processor.FileIndex;
import org.example.processor.FileProcessor;
import org.example.processor.FileTaskProcessor;
import org.example.reconciliation.DirectoryReconciler;
import org.example.reconciliation.DirectoryReconciliationService;
import org.example.reconciliation.DirectoryRecoveryHandler;
import org.example.recovery.FileRecoveryCoordinator;
import org.example.route.TaskRouter;
import org.example.watcher.FileChangeDebounce;
import org.example.watcher.FileWatcher;
import org.example.watcher.WatchRegistrar;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


public class FileIndexerService implements AutoCloseable {
    private final Path root;
    private final List<BlockingQueue<WorkerTask>> queues;
    private final FilesStat filesStat;
    private final FileIndex fileIndex;
    private final TaskRouter taskRouter;
    private final FileScanner fileScanner;
    private final ScheduledExecutorService debounceExecutor;
    private final ExecutorService rescanExecutor;
    private final ScheduledExecutorService recoveryExecutor;
    private final FileChangeDebounce debounce;
    private final Thread[] workers;
    private final Thread watcherThread;
    private static final int QUEUE_CAPACITY = 100;
    private final FileRecoveryCoordinator fileRecoveryCoordinator;
    private final DirectoryReconciliationService reconciliationService;
    private final CountDownLatch watcherReady;
    private final FileWatcher fileWatcher;
    private final Thread producerThread;
    private final FileProducer producer;
    private final Object indexStateLock = new Object();

    private FileIndexerState indexerState = FileIndexerState.NEW;
    private final Object lifecycleLock = new Object();

    FileIndexerService(
            int workerCount,
            Path root,
            int maxRecoveryAttempts,
            int queueCapacity,
            FileTaskProcessor processor
    ) {
        this(workerCount, root, maxRecoveryAttempts, queueCapacity,
                processor, new FileScanner());
    }

    FileIndexerService(
            int workerCount,
            Path root,
            int maxRecoveryAttempts,
            int queueCapacity,
            FileTaskProcessor processor,
            FileScanner scanner
    ) {
        this(workerCount, root, maxRecoveryAttempts, queueCapacity,
                processor, scanner, new FileIndex());
    }

    FileIndexerService(
            int workerCount,
            Path root,
            int maxRecoveryAttempts,
            int queueCapacity,
            FileTaskProcessor processor,
            FileScanner scanner,
            FileIndex index
    ) {
        this.root = root.toAbsolutePath().normalize();

        queues = createQueues(workerCount, queueCapacity);

        filesStat = new FilesStat();

        fileIndex = java.util.Objects.requireNonNull(index, "index");
        taskRouter = new TaskRouter(queues);
        fileScanner = java.util.Objects.requireNonNull(scanner, "scanner");

        debounceExecutor =
                Executors.newSingleThreadScheduledExecutor(runnable ->
                        new Thread(runnable, "DebounceWorker")
                );

        rescanExecutor =
                Executors.newSingleThreadExecutor(runnable ->
                        new Thread(runnable, "RescanWorker")
                );

        recoveryExecutor =
                Executors.newSingleThreadScheduledExecutor(runnable ->
                        new Thread(runnable, "RecoveryWorker")
                );

        debounce =
                new FileChangeDebounce(
                        debounceExecutor,
                        taskRouter
                );

        Map<WatchKey, Path> registeredDirectories =
                new ConcurrentHashMap<>();

        DirectoryReconciler directoryReconciler =
                new DirectoryReconciler(
                        fileScanner,
                        fileIndex,
                        taskRouter
                );

        reconciliationService =
                new DirectoryReconciliationService(
                        rescanExecutor,
                        directoryReconciler
                );

        DirectoryRecoveryHandler directoryRecoveryHandler =
                new DirectoryRecoveryHandler(
                        reconciliationService
                );

        fileRecoveryCoordinator =
                new FileRecoveryCoordinator(
                        recoveryExecutor,
                        taskRouter,
                        maxRecoveryAttempts,
                        directoryRecoveryHandler
                );


        workers = new Thread[queues.size()];
        createWorkers(processor);

        WatchRegistrar watchRegistrar =
                new WatchRegistrar(
                        registeredDirectories
                );


        watcherReady = new CountDownLatch(1);

        fileWatcher = new FileWatcher(
                this.root,
                debounce,
                taskRouter,
                watchRegistrar,
                reconciliationService,
                watcherReady
        );

        watcherThread =
                new Thread(fileWatcher, "FileWatcher");

        this.producer = new FileProducer(
                this.root,
                taskRouter,
                fileScanner
        );

        producerThread =
                new Thread(this.producer, "Producer");


    }

    public FileIndexerService(
            int workerCount,
            Path root,
            int maxRecoveryAttempts
    ) {
        this(
                workerCount,
                root,
                maxRecoveryAttempts,
                QUEUE_CAPACITY,
                FileProcessor::process
        );
    }


    private void createWorkers(FileTaskProcessor processor) {
        for (int i = 0; i < workers.length; i++) {
            FileWorker fileWorker = new FileWorker(
                    queues.get(i),
                    filesStat,
                    fileIndex,
                    fileRecoveryCoordinator,
                    processor,
                    indexStateLock
            );

            workers[i] = new Thread(
                    fileWorker,
                    "Worker-" + (i + 1)
            );
        }
    }

    public void start()
            throws InterruptedException, IOException {
        synchronized (lifecycleLock) {
            if (indexerState != FileIndexerState.NEW) {
                throw new IllegalStateException("Cannot start again");
            }

            indexerState = FileIndexerState.STARTING;
        }
        try {
            synchronized (lifecycleLock) {
                if (indexerState != FileIndexerState.STARTING) {
                    return;
                }
                startWorkers();
                watcherThread.start();
            }
            watcherReady.await();

            IOException startupFailure = fileWatcher.getStartupFailure();

            if (startupFailure != null) {
                throw startupFailure;
            }

            runInitialScan();

            awaitInitialProcessing();

            synchronized (lifecycleLock) {
                if (indexerState == FileIndexerState.STARTING) {
                    indexerState = FileIndexerState.RUNNING;
                }
            }
        } catch (InterruptedException | IOException | RuntimeException e) {
            shutdown();
            throw e;
        }
    }

    private void startWorkers() {
        for (int i = 0; i < queues.size(); i++) {
            workers[i].start();
        }
    }

    private static List<BlockingQueue<WorkerTask>> createQueues(
            int workerCount,
            int queueCapacity
    ) {
        List<BlockingQueue<WorkerTask>> queues =
                new ArrayList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            queues.add(
                    new ArrayBlockingQueue<>(queueCapacity)
            );
        }

        return queues;
    }

    private void runInitialScan() throws InterruptedException, IOException {
        synchronized (lifecycleLock) {
            if (indexerState != FileIndexerState.STARTING) {
                return;
            }
            producerThread.start();
        }

        producerThread.join();
        IOException failure = producer.getScanFailure();
        if (failure != null) {
            throw failure;
        }

    }

    private void awaitInitialProcessing()
            throws InterruptedException {
        CountDownLatch barrierReached =
                new CountDownLatch(queues.size());

        synchronized (lifecycleLock) {
            if (indexerState != FileIndexerState.STARTING) {
                return;
            }
            for (BlockingQueue<WorkerTask> queue : queues) {
                queue.put(new BarrierTask(barrierReached));
            }
        }

        barrierReached.await();
    }

    private void stopWatcher()
            throws InterruptedException {
        interruptAndJoin(watcherThread);
    }

    private void stopWorkers()
            throws InterruptedException {
        boolean wasInterrupted = false;

        for (BlockingQueue<WorkerTask> queue : queues) {
            boolean stopSent = false;
            while (!stopSent) {
                try {
                    queue.put(new StopTask());
                    stopSent = true;
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                }
            }
        }

        for (Thread thread : workers) {
            boolean workedJoin = false;

            while (!workedJoin) {
                try {
                    thread.join();
                    workedJoin = true;
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                }
            }


        }

        if (wasInterrupted) {
            throw new InterruptedException();
        }
    }

    private void stopProducer() throws InterruptedException {
        interruptAndJoin(producerThread);
    }

    private void cancelScheduledRecoveries()
            throws InterruptedException {
        recoveryExecutor.shutdownNow();

        if (!recoveryExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
            System.err.println(
                    "Recovery executor не удалось завершить"
            );
        }
    }

    public void shutdown() {
        boolean wasInterrupted = false;

        synchronized (lifecycleLock) {

            if (indexerState == FileIndexerState.STOPPED) {
                return;
            }
            while (indexerState == FileIndexerState.STOPPING) {
                try {
                    lifecycleLock.wait();
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                }
            }

            if (indexerState == FileIndexerState.STOPPED) {
                if (wasInterrupted) {
                    Thread.currentThread().interrupt();
                }

                return;
            }

            indexerState = FileIndexerState.STOPPING;
        }


        try {
            System.out.println("Начинаем остановку");

            try {
                stopWatcher();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

            try {
                stopProducer();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

            reconciliationService.shutdown();

            fileRecoveryCoordinator.shutdown();

            debounce.shutdown();

            try {
                stopExecutor(debounceExecutor);
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

            try {
                stopExecutor(rescanExecutor);
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

            try {
                cancelScheduledRecoveries();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

            try {
                stopWorkers();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }

        } finally {
            synchronized (lifecycleLock) {
                indexerState = FileIndexerState.STOPPED;
                lifecycleLock.notifyAll();
            }

            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void interruptAndJoin(Thread thread)
            throws InterruptedException {
        if (!thread.isAlive()) {
            return;
        }

        thread.interrupt();
        boolean wasInterrupted = false;

        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }
        }

        if (wasInterrupted) {
            throw new InterruptedException();
        }
    }

    private void stopExecutor(
            ExecutorService executor
    )
            throws InterruptedException {
        executor.shutdown();
        boolean wasInterrupted = false;
        boolean termination;

        while (true) {
            try {
                termination = executor.awaitTermination(30, TimeUnit.SECONDS);
                break;
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }
        }

        if (!termination) {
            executor.shutdownNow();
            while (true) {
                try {
                    termination = executor.awaitTermination(30, TimeUnit.SECONDS);
                    break;
                } catch (InterruptedException e) {
                    wasInterrupted = true;
                }
            }
        }

        if (!termination) {
            System.err.println(executor.getClass().getSimpleName() + " не завершился.");
        }

        if (wasInterrupted) {
            throw new InterruptedException();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    public FileIndexerSnapshot snapshot() {
        synchronized (indexStateLock) {
            return new FileIndexerSnapshot(
                    fileIndex.snapshotMap(),
                    filesStat.countFiles(),
                    filesStat.totalBytes(),
                    filesStat.countError()
            );
        }
    }
}
