package com.pixonic.scheduler;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Future;

public class SchedulerApp {

    public static void main(String[] args) throws Exception {
        Scheduler scheduler = new Scheduler(Runtime.getRuntime().availableProcessors());

        ArrayList<Future<String>> tasks = new ArrayList<>();
        tasks.add(scheduler.submit(LocalDateTime.now().plusSeconds(10), () -> "Hello world!(after 10 seconds)"));
        tasks.add(scheduler.submit(LocalDateTime.now().plusSeconds(15), () -> "Hello world!(after 15 seconds)"));
        tasks.add(scheduler.submit(LocalDateTime.now(), () -> "Hello world!(immediately)"));

        while (!tasks.isEmpty()) {
            for (Iterator<Future<String>> iterator = tasks.iterator(); iterator.hasNext(); ) {
                Future<String> task = iterator.next();
                if (task.isDone()) {
                    System.out.println(task.get());
                    iterator.remove();
                }
            }
        }

        scheduler.shutdown();
    }
}
