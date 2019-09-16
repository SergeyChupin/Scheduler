package com.pixonic.scheduler;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Scheduler {

    private final BlockingQueue<Runnable> workersQueue;

    private final Collection<Thread> threadPool;

    private final Object waitLock;

    public Scheduler(int workersCount) {
        this(workersCount, new SchedulerThreadFactory());
    }

    public Scheduler(int workersCount, ThreadFactory threadFactory) {
        this.workersQueue = new LinkedBlockingQueue<>();
        this.waitLock = new Object();
        this.threadPool = IntStream.range(0, workersCount)
                .mapToObj(__ -> createWorkerThread(threadFactory))
                .peek(Thread::start)
                .collect(Collectors.toList());
    }

    public <V> Future<V> submit(LocalDateTime dateTime, Callable<V> work) {
        FutureTask<V> futureTask = new FutureTask<>(work);
        workersQueue.add(futureTask);
        synchronized (waitLock) {
            waitLock.notify();
        }
        return futureTask;
    }

    private Thread createWorkerThread(ThreadFactory threadFactory) {
        return threadFactory.newThread(new Worker());
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    Runnable work = workersQueue.poll();
                    if (work != null) {
                        work.run();
                    }
                    synchronized (waitLock) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                // nop
            }
        }
    }

    private static class SchedulerThreadFactory implements ThreadFactory {

        private static final String NAME_PREFIX = "scheduler-thread-";

        private final AtomicInteger threadNumber;

        public SchedulerThreadFactory() {
            this.threadNumber = new AtomicInteger(1);
        }

        public Thread newThread(Runnable work) {
            Thread thread = new Thread(work, NAME_PREFIX + this.threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}

