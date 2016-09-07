package com.tromix.exam.orders.a;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.*;

public class PriorityOrderProcessor implements ScheduledExecutorService {

    private Queue<RunnableScheduledFuture<?>> incomingTasks = new ConcurrentLinkedQueue<>();
    private PriorityQueue<RunnableScheduledFuture<?>> delayedTasks = new PriorityQueue<>();
    private ThreadPoolExecutor executor;

    private final Runnable scheduler = this::doStateMaintenance;

    public PriorityOrderProcessor(int threadCount) {
        executor = new ThreadPoolExecutor(threadCount, threadCount, 4, TimeUnit.SECONDS, new LinkedTransferQueue<>());
        executor.setThreadFactory((r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        executor.execute(scheduler);
    }

    private void doStateMaintenance() {
        while (!isShutdown()) {
            RunnableScheduledFuture<?> toSchedule;
            while ((toSchedule = incomingTasks.poll()) != null) {
                delayedTasks.add(toSchedule);
            }
            RunnableScheduledFuture<?> toExecute;
            while ((toExecute = delayedTasks.peek()) != null && toExecute.getDelay(TimeUnit.NANOSECONDS) <= 0) {
                delayedTasks.poll();
                executor.execute(toExecute);
            }
        }

        if (!isShutdown()) {
            executor.execute(scheduler);
        }
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        FutureJob<?> future = new FutureJob<Void>(command, delay, unit);
        incomingTasks.add(future);

        return future;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        FutureJob<V> future = new FutureJob<>(callable, delay, unit);
        incomingTasks.add(future);

        return future;
    }

    public <V> void schedule(Callable<V> command, LocalDateTime target) {
        schedule(command, getTimeFromNowInNanos(target), TimeUnit.NANOSECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        if (period < 0) {
            throw new IllegalArgumentException("the 'period' value can't be negative");
        }
        FutureJob<?> future = new FutureJob<Void>(command, initialDelay, period, unit);
        incomingTasks.add(future);

        return future;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        if (delay < 0) {
            throw new IllegalArgumentException("the 'delay' value can't be negative");
        }
        FutureJob<?> future = new FutureJob<Void>(command, initialDelay, -delay, unit);
        incomingTasks.add(future);

        return future;
    }

    public long getTimeFromNowInNanos(LocalDateTime target) {
        if (target == null) {
            throw new IllegalArgumentException("the 'time' can't be null");
        }
        return ChronoUnit.NANOS.between(LocalDateTime.now(), target);
    }

    private class FutureJob<T> extends FutureTask<T> implements RunnableScheduledFuture<T> {
        private final long period;
        private long nanos;

        FutureJob(Runnable r, long delay, TimeUnit u) {
            super(r, null);
            this.nanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(delay, u);
            this.period = 0;
        }

        FutureJob(Callable<T> callable, long delay, TimeUnit u) {
            super(callable);
            this.nanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(delay, u);
            period = 0;
        }

        FutureJob(Runnable command, long initialDelay, long period, TimeUnit unit) {
            super(command, null);
            this.nanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(initialDelay, unit);
            this.period = TimeUnit.NANOSECONDS.convert(period, unit);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(nanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o1) {
            long delta;
            if (o1 instanceof FutureJob) {
                FutureJob<?> futureJob = (FutureJob<?>) o1;
                delta = this.nanos - futureJob.nanos;
            } else {
                delta = getDelay(TimeUnit.NANOSECONDS) - o1.getDelay(TimeUnit.NANOSECONDS);
            }

            return (delta == 0) ? 0 : (delta < 0) ? -1 : 1;
        }

        @Override
        public boolean isPeriodic() {
            return period != 0;
        }

        @Override
        public void run() {
            if (isPeriodic()) {
                if (runAndReset()) {
                    if (period < 0) {
                        nanos = System.nanoTime() + (-period);
                    } else {
                        nanos += period;
                    }
                    incomingTasks.add(this);
                }
            } else {
                super.run();
            }
        }
    }
}
