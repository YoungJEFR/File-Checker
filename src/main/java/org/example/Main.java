package org.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Введите путь к папке:");

        Path root = Path.of(scanner.nextLine());

        if (!Files.exists(root) || !Files.isDirectory(root)) {
            System.out.println("Папка не найдена");
            return;
        }

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
                    new FileWorker(queue);

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
        System.out.println("Все файлы обработаны.");

        scanner.close();
    }
}