package org.example;

import com.formdev.flatlaf.FlatDarkLaf;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

public class Main {
    static void main() throws IOException {
        Scanner in = new Scanner(System.in);
        Path path;

        while (true) {
            String input = in.nextLine();

            if (input.equalsIgnoreCase("EXIT")) {
                return;
            }

            path = Paths.get(input);

            if (Files.exists(path) && Files.isDirectory(path)) {
                break;
            }

            System.out.println("Такой папки нет. Попробуйте снова:");
        }

        FileScanner fileScanner = new FileScanner(path);

        List<FileInfo> allFiles = fileScanner.scanFile();

        System.out.println("Всего файлов: " + allFiles.size());

        for (FileInfo fileInfo : allFiles) {
            System.out.println(fileInfo);
        }

    }
}
