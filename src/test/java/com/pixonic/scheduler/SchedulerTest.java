package com.pixonic.scheduler;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
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

    private Scheduler scheduler;

    private ExecutorService executor;

    @Before
    public void beforeUp() {
        scheduler = new Scheduler(Runtime.getRuntime().availableProcessors());

        if (maxProducers > 1) {
            executor = Executors.newFixedThreadPool(maxProducers);
        } else {
            executor = new CurrentThreadExecutor();
        }
    }

    @After
    public void afterDown() {
        executor.shutdown();
        scheduler.shutdown();
    }

    @Test(timeout = 120_000)
    public void baseTest() throws Exception {
        ConcurrentLinkedQueue<Future> tasks = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < maxScheduledTasks; i++) {
            executor.submit(() -> {
                Future task = scheduler.submit(randomScheduledTime(), workFactory.createWork());
                tasks.add(task);
            });
        }

        CountDownLatch finishedScheduledTasks = new CountDownLatch(maxScheduledTasks);
        new Thread(() -> {
            while (true) {
                for (Iterator<Future> iterator = tasks.iterator(); iterator.hasNext(); ) {
                    Future task = iterator.next();
                    if (task.isDone()) {
                        iterator.remove();
                        finishedScheduledTasks.countDown();
                    }
                }
                try {
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        finishedScheduledTasks.await(120, TimeUnit.SECONDS);

        Assert.assertEquals(0, finishedScheduledTasks.getCount());
    }

    private static LocalDateTime randomScheduledTime() {
        return LocalDateTime.now().plusSeconds(ThreadLocalRandom.current().nextInt(10));
    }

    public static class CurrentThreadExecutor extends AbstractExecutorService {

        public void execute(Runnable r) {
            r.run();
        }

        @Override
        public void shutdown() {
            // nop
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
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
