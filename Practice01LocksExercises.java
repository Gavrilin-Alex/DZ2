import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;




class P1DiningPhilosophersHomework {

    private static final class Fork {
        private final int id;
        private final Lock lock = new ReentrantLock(true);

        private Fork(int id) {
            this.id = id;
        }
    }

    static final class Table {
        private final Fork[] forks;
        private final AtomicIntegerArray forkUsers;
        private final AtomicBoolean conflictDetected = new AtomicBoolean();

        Table(int philosopherCount) {
            forks = new Fork[philosopherCount];
            for (int id = 0; id < philosopherCount; id++) {
                forks[id] = new Fork(id);
            }
            forkUsers = new AtomicIntegerArray(philosopherCount);
        }

        void eat(int philosopherId) {
            Fork left = forks[philosopherId];
            Fork right = forks[(philosopherId + 1) % forks.length];

            Fork firstLock = left.id < right.id ? left : right;
            Fork secondLock = left.id < right.id ? right : left;

            firstLock.lock.lock();
            try {
                secondLock.lock.lock();
                try {
                    eatWithBothForks(left, right);
                } finally {
                    secondLock.lock.unlock();
                }
            } finally {
                firstLock.lock.unlock();
            }
        }

        private void eatWithBothForks(Fork left, Fork right) {
            int leftUsers = forkUsers.incrementAndGet(left.id);
            int rightUsers = forkUsers.incrementAndGet(right.id);
            if (leftUsers != 1 || rightUsers != 1) {
                conflictDetected.set(true);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                forkUsers.decrementAndGet(right.id);
                forkUsers.decrementAndGet(left.id);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int philosopherCount = 5;
        int mealsPerPhilosopher = 100;
        Table table = new Table(philosopherCount);
        ExecutorService pool = Executors.newFixedThreadPool(philosopherCount);
        List<Future<Integer>> results = new ArrayList<>();

        try {
            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int philosopherId = philosopher;
                results.add(pool.submit(() -> {
                    int meals = 0;
                    while (meals < mealsPerPhilosopher) {
                        table.eat(philosopherId);
                        meals++;
                        Thread.yield();
                    }
                    return meals;
                }));
            }

            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int meals = results.get(philosopher).get(10, TimeUnit.SECONDS);
                if (meals != mealsPerPhilosopher) {
                    throw new AssertionError(
                            "Philosopher " + philosopher + " ate " + meals + " times");
                }
            }
        } catch (TimeoutException e) {
            throw new AssertionError("Possible deadlock: philosophers did not finish", e);
        } finally {
            pool.shutdownNow();
        }

        if (table.conflictDetected.get()) {
            throw new AssertionError("Two philosophers used the same fork");
        }

        System.out.println("OK: every philosopher ate " + mealsPerPhilosopher
                + " times, no deadlock detected");
    }
}
/*Нет, в обоих случаях взаимную блокировку (deadlock) организовать нельзя.
 *Для предотвращения дедлока важен только порядок захвата ресурсов.
 *Если все потоки берут их строго по одной очереди (например, строго от меньшего id к большему),
 *циклическое ожидание физически невозможно.
 *
 *Порядок освобождения ресурсов (хоть в обратном, хоть в том же самом) на дедлок уже никак не влияет,
 *так как ресурсы к этому моменту уже успешно захвачены и поток их просто отдает.
 */