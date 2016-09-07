package com.tromix.exam.orders.a;


import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Callable;

public class Main {

    private final static DateTimeFormatter df = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();
        PriorityOrderProcessor exec = new PriorityOrderProcessor(3);

        System.out.println("Now: " + LocalTime.now().format(df));
        for (int i = 0; i < 3; i++) {
            new Thread(() -> {
                try {
                    Thread.sleep(random.nextInt(3) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int j = 0; j < 3; j++) {
                    LocalDateTime targetTime = LocalDateTime.now().plusSeconds(random.nextInt(30) - 10);
                    String time = targetTime.format(df);
                    System.out.println("Scheduling: #" + time + " at " + LocalDateTime.now().format(df));
                    Order order = new Order(time);
                    exec.schedule(order, targetTime);
                }
            }).start();
        }
        Thread.sleep(30000);
        exec.shutdown();
    }

    private static class Order implements Callable {

        private String name;

        Order(String name) {
            this.name = name;
        }

        @Override
        public Void call() throws Exception {
            System.out.println("Executing: #" + name + " at " + LocalDateTime.now().format(df));
            Thread.sleep(1000);
            return null;
        }
    }
}
