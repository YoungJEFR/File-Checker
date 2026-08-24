package org.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);

        Path path;

        System.out.println("Введите путь к папке или EXIT для выхода:");

        while (true) {
            String input = in.nextLine();

            if (input.equalsIgnoreCase("EXIT")) {
                return;
            }

            path = Path.of(input);

            if (Files.exists(path) && Files.isDirectory(path)) {
                break;
            }

            System.out.println("Такой папки нет. Попробуйте снова:");
        }


        FileScanner fileScanner = new FileScanner(path);

        List<FileInfo> oldFiles = fileScanner.scanFile();

        System.out.println("\nПервое сканирование завершено.");
        System.out.println("Всего файлов: " + oldFiles.size());

        for (FileInfo fileInfo : oldFiles) {
            System.out.println(fileInfo);
        }


        System.out.println("""
                
                Теперь измени файлы в папке:
                - создай файл
                - измени файл
                - удали файл
                
                После этого нажми ENTER.
                """);

        in.nextLine();


        List<FileInfo> newFiles = fileScanner.scanFile();


        FileChangeDetector detector = new FileChangeDetector();

        List<FileChange> changes =
                detector.detectChanges(oldFiles, newFiles);

        System.out.println("\nНайдено изменений: " + changes.size());

        if (changes.isEmpty()) {
            System.out.println("Изменений нет.");
        } else {
            for (FileChange change : changes) {
                System.out.println(change);
            }
        }

        in.close();
    }
}