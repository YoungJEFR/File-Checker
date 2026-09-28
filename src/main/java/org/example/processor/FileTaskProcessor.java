package org.example.processor;

import org.example.model.FileInfo;
import org.example.model.FileTask;

import java.io.IOException;

@FunctionalInterface
public interface FileTaskProcessor {

    FileInfo process(FileTask fileTask)
            throws IOException, InterruptedException;
}
