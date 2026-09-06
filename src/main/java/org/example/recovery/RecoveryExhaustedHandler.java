package org.example.recovery;

import org.example.model.FileTask;

import java.io.IOException;

@FunctionalInterface
public interface RecoveryExhaustedHandler {
    void handle(FileTask fileTask, IOException cause);
}
