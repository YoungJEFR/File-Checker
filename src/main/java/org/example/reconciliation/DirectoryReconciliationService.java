package org.example.reconciliation;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class DirectoryReconciliationService {
    private final ExecutorService executorService;
    private final DirectoryReconciler directoryReconciler;

    private final ConcurrentHashMap<Path, ReconciliationStatus> reconciliationStatusMap
            = new ConcurrentHashMap<>();

    public DirectoryReconciliationService(
            ExecutorService executorService,
            DirectoryReconciler directoryReconciler
    ) {
        this.executorService = executorService;
        this.directoryReconciler = directoryReconciler;
    }

    public void request(Path directory) {
        AtomicBoolean shouldStart = new AtomicBoolean(false);
        Path normalizedDirectory = directory.normalize().toAbsolutePath();

        reconciliationStatusMap.compute(normalizedDirectory, (path, reconciliationStatus) -> {
            if (reconciliationStatus == null) {
                shouldStart.set(true);
                return ReconciliationStatus.RUNNING;
            }

            return ReconciliationStatus.RUNNING_AGAIN;
        });

        if (shouldStart.get()) {
            executorService.submit(() -> {
                runReconciliation(normalizedDirectory);
            });
        }
    }

    private void runReconciliation(Path directory) {
        while(true){
            AtomicBoolean shouldRunAgain = new AtomicBoolean(false);

            try {
                directoryReconciler.reconcile(directory);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                reconciliationStatusMap.remove(directory);
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }

            reconciliationStatusMap.compute(directory, (path, reconciliationStatus) -> {
                if (reconciliationStatus == ReconciliationStatus.RUNNING_AGAIN) {
                    shouldRunAgain.set(true);
                    return ReconciliationStatus.RUNNING;
                }

                return null;
            });

            if (!shouldRunAgain.get()) {
                return;
            }
        }
    }
}
