package com.pixonic.scheduler;

import com.pixonic.scheduler.utils.LocalDateTimeUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Scheduler extends Thread {

    private final static Logger log = LogManager.getLogger(Scheduler.class);

    private final PriorityQueue<ScheduledTask> scheduledTasksQueue;

    private final BlockingQueue<ScheduledTask> workersQueue;

    private final Collection<Thread> threadPool;

    private volatile boolean shutdown;

    private long nextScheduledTime;

    public Scheduler(int workersCount) {
        this(workersCount, new SchedulerThreadFactory());
    }

    public Scheduler(int workersCount, ThreadFactory threadFactory) {
        this.shutdown = false;
        this.nextScheduledTime = Long.MAX_VALUE;
        this.scheduledTasksQueue = new PriorityQueue<>();
        this.workersQueue = new LinkedBlockingQueue<>();
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
            ScheduledTask scheduledTask;
            while (!Thread.interrupted() && !isShutdown()) {
                synchronized (this) {
                    scheduledTask = scheduledTasksQueue.peek();
                    if (scheduledTask != null) {
                        long timeout = scheduledTask.getTime() - System.currentTimeMillis();
                        if (timeout <= 0) {
                            scheduledTask = scheduledTasksQueue.poll();
                        } else {
                            nextScheduledTime = scheduledTask.getTime();
                            scheduledTask = null;
                            this.wait(timeout);
                        }
                    } else {
                        nextScheduledTime = Long.MAX_VALUE;
                        this.wait();
                    }
                }
                if (scheduledTask != null) {
                    executeTask(scheduledTask);
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
    }

    private void scheduleTask(ScheduledTask scheduledTask) {
        if (log.isInfoEnabled()) {
            log.info("Schedule task on {}", LocalDateTimeUtils.fromMillis(scheduledTask.getTime(), ZoneOffset.systemDefault()));
        }
        synchronized (this) {
            scheduledTasksQueue.add(scheduledTask);
            if (scheduledTask.getTime() < nextScheduledTime) {
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
                    ScheduledTask scheduledTask = workersQueue.take();
                    if (log.isInfoEnabled()) {
                        log.info("Process task on {}", LocalDateTimeUtils.fromMillis(scheduledTask.getTime(), ZoneOffset.systemDefault()));
                    }
                    scheduledTask.run();
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

