package com.pixonic.scheduler;

import java.time.LocalDateTime;
import java.util.concurrent.Future;

public class SchedulerApp {

    public static void main(String[] args) throws Exception {
        Scheduler scheduler = new Scheduler(Runtime.getRuntime().availableProcessors());

        Future<String> task = scheduler.submit(LocalDateTime.now(), () -> "Hello world!");

        System.out.println(task.get());
    }
}
