package org.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileScanner {

    private final Path path;

    public FileScanner(Path path) {
        this.path = path;
    }

    public List<FileInfo> scanFile() {
        List<FileInfo> fileInfoList = new ArrayList<>();

        List<Path> files = new ArrayList<>();

        try(Stream<Path> getAllFiles = Files.walk(path)) {
            files = getAllFiles
                    .filter(Files::isRegularFile)
                    .filter(path1 -> path1
                            .toString()
                            .endsWith(".md"))
                    .toList();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        for (Path path1 : files) {
            try {
                fileInfoList.add(new FileInfo(
                        Files.getLastModifiedTime(path1),
                        path1.getFileName().toString(),
                        Files.size(path1),
                        path1.toRealPath()
                ));
            } catch (IOException e) {
                e.printStackTrace();
                e.getMessage();
            }
        }

        return fileInfoList;
    }
}
