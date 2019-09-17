package com.pixonic.scheduler;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SchedulerTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {1, 13, new WorkFactoryImpl()}, {2, 1000, new WorkFactoryImpl()}, {4, 10_000, new WorkFactoryImpl()}
        });
    }

    @Parameterized.Parameter
    public int maxProducers;

    @Parameterized.Parameter(1)
    public int maxScheduledTasks;

    @Parameterized.Parameter(2)
    public WorkFactory<?> workFactory;

    @Test
    public void baseTest() throws Exception {
        Scheduler scheduler = new Scheduler(Runtime.getRuntime().availableProcessors());

        Consumer<Runnable> submitter;
        if (maxProducers > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(maxProducers);
            submitter = executor::submit;
        } else {
            submitter = this::call;
        }

        ConcurrentLinkedQueue<Future> tasks = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < maxScheduledTasks; i++) {
            submitter.accept(() -> {
                Future task = scheduler.submit(randomScheduledTime(), workFactory.createWork());
                tasks.add(task);
            });
        }

        CountDownLatch latch = new CountDownLatch(maxScheduledTasks);
        new Thread(() -> {
            while (true) {
                for (Iterator<Future> iterator = tasks.iterator(); iterator.hasNext(); ) {
                    Future task = iterator.next();
                    if (task.isDone()) {
                        latch.countDown();
                        iterator.remove();
                    }
                }
                try {
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        latch.await(120, TimeUnit.SECONDS);

        scheduler.shutdown();

        Assert.assertEquals(0, latch.getCount());
    }

    private void call(Runnable work) {
        try {
            work.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LocalDateTime randomScheduledTime() {
        return LocalDateTime.now().plusSeconds((long) Math.floor(10 * Math.random()));
    }

    private interface WorkFactory<V> {

        Callable<V> createWork();
    }

    private static class WorkFactoryImpl implements WorkFactory<String> {

        @Override
        public Callable<String> createWork() {
            return () -> "Hello world";
        }
    }
}
