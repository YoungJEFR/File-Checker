package org.example.filescanner;

import org.example.model.FileInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class FileScanner {

    @FunctionalInterface
    public interface FileHandler{
        void handle(Path path) throws InterruptedException;
    }

    public void scanFile(Path root, FileHandler handler)
            throws InterruptedException, IOException{
        try(Stream<Path> stream = Files.walk(root)) {
            Iterator<Path> iterator = stream
                    .filter(Files::isRegularFile)
                    .filter(this::isMarkdown)
                    .iterator();

            while (iterator.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                handler.handle(iterator.next());
            }
        }
    }

    private boolean isMarkdown(Path path){
        return path
                .getFileName()
                .toString()
                .endsWith(".md");
    }
}
