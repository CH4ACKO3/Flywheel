package dev.engine_room.flywheel.impl.task;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

}
