package com.pixonic.scheduler;

import com.pixonic.scheduler.utils.LocalDateTimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Scheduler extends Thread {

    private final static Logger log = LogManager.getLogger(Scheduler.class);

    private final PriorityBlockingQueue<ScheduledTask> scheduledTasksQueue;

    private final Queue<ScheduledTask> workersQueue;

    private final Collection<Thread> threadPool;

    private volatile boolean shutdown;

    private volatile long nextScheduledTime;

    private final Object waitLock;

    public Scheduler(int workersCount) {
        this(workersCount, new SchedulerThreadFactory());
    }

    public Scheduler(int workersCount, ThreadFactory threadFactory) {
        this.shutdown = false;
        this.nextScheduledTime = Long.MAX_VALUE;
        this.scheduledTasksQueue = new PriorityBlockingQueue<>();
        this.workersQueue = new ConcurrentLinkedQueue<>();
        this.waitLock = new Object();
        this.threadPool = IntStream.range(0, workersCount)
                .mapToObj(__ -> createWorkerThread(threadFactory))
                .peek(Thread::start)
                .collect(Collectors.toList());
        this.start();
    }

    @Override
    public void run() {
        log.debug("Scheduler task started");
        try {
            while (!Thread.interrupted() && !isShutdown()) {
                ScheduledTask scheduledTask = scheduledTasksQueue.take();
                long timeout = scheduledTask.getTime() - System.currentTimeMillis();
                if (timeout <= 0) {
                    executeTask(scheduledTask);
                } else {
                    log.debug("Return schedule task to queue");
                    scheduledTasksQueue.add(scheduledTask);
                    synchronized (this) {
                        this.wait(timeout);
                    }
                }
            }
        } catch (InterruptedException e) {
            // nop
        } finally {
            log.debug("Scheduler task stopped");
        }
    }

    public <V> Future<V> submit(LocalDateTime dateTime, Callable<V> task) {
        Objects.requireNonNull(dateTime, "DateTime should be specified");
        Objects.requireNonNull(task, "Task should be specified");
        if (isShutdown()) {
            throw new IllegalStateException("Unable submit task, cause: scheduler stopped!");
        }
        long millis = LocalDateTimeUtils.toMillis(dateTime, ZoneOffset.systemDefault());
        ScheduledTask<V> scheduledTask = new ScheduledTask<>(millis, task);
        if (millis < System.currentTimeMillis()) {
            executeTask(scheduledTask);
        } else {
            scheduleTask(scheduledTask);
        }
        return scheduledTask;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void shutdown() {
        shutdown = true;
        for (Thread thread : threadPool) {
            thread.interrupt();
        }
        this.interrupt();
    }

    private void executeTask(ScheduledTask scheduledTask) {
        if (log.isInfoEnabled()) {
            log.info("Execute task on {}", LocalDateTimeUtils.fromMillis(scheduledTask.getTime(), ZoneOffset.systemDefault()));
        }
        workersQueue.add(scheduledTask);
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    private void scheduleTask(ScheduledTask scheduledTask) {
        if (log.isInfoEnabled()) {
            log.info("Schedule task on {}", LocalDateTimeUtils.fromMillis(scheduledTask.getTime(), ZoneOffset.systemDefault()));
        }
        scheduledTasksQueue.add(scheduledTask);
        if (scheduledTask.getTime() < nextScheduledTime) {
            synchronized (this) {
                this.notify();
            }
        }
    }

    private Thread createWorkerThread(ThreadFactory threadFactory) {
        return threadFactory.newThread(new Worker());
    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            log.debug("Worker task started");
            try {
                while (!Thread.interrupted() && !isShutdown()) {
                    synchronized (waitLock) {
                        waitLock.wait();
                    }
                    ScheduledTask scheduledTask = workersQueue.poll();
                    if (scheduledTask != null) {
                        if (log.isInfoEnabled()) {
                            log.info("Process task on {}", LocalDateTimeUtils.fromMillis(scheduledTask.getTime(), ZoneOffset.systemDefault()));
                        }
                        scheduledTask.run();
                    }
                }
            } catch (InterruptedException e) {
                // nop
            } finally {
                log.debug("Worker task stopped");
            }
        }
    }

    private static class ScheduledTask<V> extends FutureTask<V> implements Comparable<ScheduledTask> {

        private final static AtomicLong seq = new AtomicLong();

        private final long time;

        private final long seqNum;

        private ScheduledTask(long time, Callable<V> callable) {
            super(callable);
            this.time = time;
            this.seqNum = seq.getAndIncrement();
        }

        private long getTime() {
            return time;
        }

        @Override
        public int compareTo(ScheduledTask task) {
            int res = Long.compare(time, task.time);
            if (res == 0) {
                res = seqNum < task.seqNum ? -1 : 1;
            }
            return res;
        }
    }

    private static class SchedulerThreadFactory implements ThreadFactory {

        private static final String NAME_PREFIX = "scheduler-thread-";

        private final AtomicInteger threadNumber;

        private SchedulerThreadFactory() {
            this.threadNumber = new AtomicInteger(1);
        }

        public Thread newThread(Runnable task) {
            Thread thread = new Thread(task, NAME_PREFIX + this.threadNumber.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}

