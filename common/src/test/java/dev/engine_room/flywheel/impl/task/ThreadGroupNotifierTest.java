package dev.engine_room.flywheel.impl.task;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class ThreadGroupNotifierTest {
	@Test
	void notificationBetweenConditionAndParkIsNotLost() throws Exception {
		ThreadGroupNotifier notifier = new ThreadGroupNotifier();
		AtomicBoolean waiting = new AtomicBoolean(true);
		CountDownLatch checked = new CountDownLatch(1);
		CountDownLatch signalled = new CountDownLatch(1);
		Thread worker = new Thread(() -> notifier.awaitNotification(() -> {
			boolean result = waiting.get();
			if (result) {
				checked.countDown();
				// Do not park here: it would consume the permit under test.
				while (signalled.getCount() != 0) Thread.onSpinWait();
			}
			return result;
		}));
		worker.setDaemon(true);
		notifier.register(worker);
		worker.start();
		try {
			assertTrue(checked.await(2, TimeUnit.SECONDS));
			waiting.set(false);
			notifier.postNotification();
			signalled.countDown();
			worker.join(2000);
			assertFalse(worker.isAlive(), "Notification before park was lost");
		} finally {
			waiting.set(false);
			signalled.countDown();
			worker.interrupt();
			worker.join(2000);
		}
	}

	@Test
	void senderDoesNotAcquireWorkerMonitor() throws Exception {
		ThreadGroupNotifier notifier = new ThreadGroupNotifier();
		CountDownLatch locked = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		Thread worker = new Thread(() -> {
			synchronized (notifier) {
				locked.countDown();
				try { release.await(); } catch (InterruptedException ignored) {}
			}
		});
		worker.setDaemon(true);
		worker.start();
		CountDownLatch sent = new CountDownLatch(1);
		Thread sender = new Thread(() -> { notifier.postNotification(); sent.countDown(); });
		sender.setDaemon(true);
		try {
			assertTrue(locked.await(2, TimeUnit.SECONDS));
			sender.start();
			assertTrue(sent.await(2, TimeUnit.SECONDS));
		} finally {
			release.countDown();
			worker.join(2000);
			sender.join(2000);
		}
	}

	@Test
	void idleWorkersDoNotPollAndAllWakeOnShutdown() throws Exception {
		ThreadGroupNotifier notifier = new ThreadGroupNotifier();
		AtomicBoolean running = new AtomicBoolean(true);
		AtomicInteger checks = new AtomicInteger();
		Thread[] workers = new Thread[8];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new Thread(() -> notifier.awaitNotification(() -> {
				checks.incrementAndGet();
				return running.get();
			}));
			workers[i].setDaemon(true);
			notifier.register(workers[i]);
			workers[i].start();
		}
		try {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			for (Thread worker : workers) {
				while (worker.getState() != Thread.State.WAITING && System.nanoTime() < deadline) Thread.yield();
				assertEquals(Thread.State.WAITING, worker.getState());
			}
			int before = checks.get();
			Thread.sleep(100);
			// Spurious wakes are legal, but regular 10 ms polling is not.
			assertTrue(checks.get() - before < workers.length * 2);
			running.set(false);
			notifier.postNotification();
			for (Thread worker : workers) {
				worker.join(2000);
				assertFalse(worker.isAlive());
			}
		} finally {
			running.set(false);
			for (Thread worker : workers) worker.interrupt();
			for (Thread worker : workers) worker.join(2000);
			notifier.clear();
		}
	}

	@Test
	void stoppedConditionDoesNotWaitForAnotherNotification() throws Exception {
		ThreadGroupNotifier notifier = new ThreadGroupNotifier();
		Thread worker = new Thread(() -> notifier.awaitNotification(() -> false));
		worker.setDaemon(true);
		notifier.register(worker);
		worker.start();
		worker.join(2000);
		assertFalse(worker.isAlive());
	}

	@Test
	void interruptIsConsumedRatherThanSpinning() throws Exception {
		ThreadGroupNotifier notifier = new ThreadGroupNotifier();
		AtomicBoolean stillInterrupted = new AtomicBoolean(true);
		Thread worker = new Thread(() -> {
			Thread.currentThread().interrupt();
			notifier.awaitNotification(() -> true);
			stillInterrupted.set(Thread.currentThread().isInterrupted());
		});
		worker.setDaemon(true);
		notifier.register(worker);
		worker.start();
		worker.join(2000);
		assertFalse(worker.isAlive());
		assertFalse(stillInterrupted.get());
	}

	@Test
	void executorCanStopAndRestartAfterConcurrentWork() throws Exception {
		ParallelTaskExecutor executor = new ParallelTaskExecutor("NotifierTest", 8);
		for (int round = 0; round < 5; round++) {
			executor.startWorkers();
			try {
				CountDownLatch done = new CountDownLatch(10000);
				for (int i = 0; i < 10000; i++) executor.execute(done::countDown);
				assertTrue(done.await(2, TimeUnit.SECONDS));
			} finally {
				executor.stopWorkers();
			}
		}
	}
}
