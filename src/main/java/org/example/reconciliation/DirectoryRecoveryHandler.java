package org.example.reconciliation;

import org.example.model.FileTask;
import org.example.recovery.RecoveryExhaustedHandler;

import java.io.IOException;
import java.nio.file.Path;

public class DirectoryRecoveryHandler implements RecoveryExhaustedHandler {
    private final DirectoryReconciliationService reconciliationService;

    public DirectoryRecoveryHandler(
          DirectoryReconciliationService reconciliationService
    ) {
        this.reconciliationService = reconciliationService;
    }

    @Override
    public void handle(FileTask fileTask, IOException cause) {
        Path parentPath = fileTask.path().getParent();
        if (parentPath == null) {
            System.out.println(cause.getMessage());
            return;
        }

        reconciliationService.request(parentPath);
    }
}
