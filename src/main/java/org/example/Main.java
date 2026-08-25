package org.example;

import org.example.model.FileInfo;
import org.example.model.FileTask;
import org.example.model.FilesStat;
import org.example.pipeline.FileProducer;
import org.example.pipeline.FileWorker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Main {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Введите путь к папке:");

        Path root = Path.of(scanner.nextLine());

        if (!Files.exists(root) || !Files.isDirectory(root)) {
            System.out.println("Папка не найдена");
            return;
        }

        AtomicInteger countFile = new AtomicInteger(0);
        AtomicInteger errorFiles = new AtomicInteger(0);
        LongAdder countByteFiles = new LongAdder();
        FilesStat filesStat = new FilesStat(countFile, errorFiles, countByteFiles);
        ConcurrentHashMap<Path, FileInfo> indexMap = new ConcurrentHashMap<>();

        int workerCount = 3;

        BlockingQueue<FileTask> queue =
                new ArrayBlockingQueue<>(3);

        FileProducer producer =
                new FileProducer(root, queue);

        Thread producerThread =
                new Thread(producer, "Producer");


        Thread[] workers = new Thread[workerCount];

        for (int i = 0; i < workerCount; i++) {

            FileWorker worker =
                    new FileWorker(queue, indexMap, filesStat);

            workers[i] = new Thread(
                    worker,
                    "Worker-" + (i + 1)
            );

            workers[i].start();
        }


        producerThread.start();


        try {
            producerThread.join();

            for (int i = 0; i < workerCount; i++) {
                queue.put(
                        new FileTask(Path.of("STOP"))
                );
            }

            for (Thread worker : workers) {
                worker.join();
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        System.out.println("Успешно обработанных файлов: " + filesStat.getCountFiles() + "\n" +
                " | Ошибок: " +  filesStat.getErrorFiles() + "\n" +
                " | Всего количество байт: " +  filesStat.getCountByteFiles());

        indexMap.forEach((path, fileInfo) -> {
            System.out.println(path + " : " + fileInfo);
        });

        System.out.println("Все файлы обработаны.");

        scanner.close();
    }
}