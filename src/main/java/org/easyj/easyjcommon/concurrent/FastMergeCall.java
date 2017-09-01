package org.easyj.easyjcommon.concurrent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.function.Supplier;

import org.easyj.easyjlog.util.LoggerUtil;

/**
 * 合并调用（改进版） <br>
 * 
 * 与MergeCall的区别就是FastMergeCall内部使用同步器替代了计数器，同步器中包含了一个等待线程队列管理。<br>
 * 
 * <br>
 * 使用示例： <code>
 * FastMergeCall.execute("key", () -> {
 *     return ref.invoke();
 * });
 * </code>
 * 
 * @author dengjianjun
 *
 * @see org.easyj.easyjcommon.concurrent.MergeCall
 */
public class FastMergeCall {

	// 最大并发请求数，超过部分快速失败
	private static long MAX_REQUEST = 10000;
	// 合并并发请求中最大错误重试次数
	private static long MAX_ERROR = 3;
	// 最大等待时间，单位：毫秒
	private static long MAX_TIMEOUT = 3 * 1000;

	// 同步器集合，定义为 volatile 是为了在 double-check 的时候禁止指令重排
	private static volatile Map<String, Sync> syncMap = new ConcurrentHashMap<>();

	private static FastMergeCall call = new FastMergeCall();

	public static <E extends Object> E execute(String key, Supplier<E> function) {
		return call.executeCall(key, function);
	}

	@SuppressWarnings("unchecked")
	private <E extends Object> E executeCall(String key, Supplier<E> function) {
		if (key == null || key.trim().length() == 0) {
			return null;
		}

		Sync sync = syncMap.get(key);
		if (sync == null) {
			// 初始对应的计数器
			synchronized (syncMap) {
				sync = syncMap.get(key);
				if (sync == null) {
					sync = new Sync(MAX_REQUEST, MAX_ERROR, TimeUnit.MILLISECONDS.toNanos(MAX_TIMEOUT));
					syncMap.put(key, sync);
				}
			}
		}

		Optional<Object> result = null;

		while (result == null) {
			try {
				if (!sync.countUpOrAwait()) {
					// 超时或快速失败退出
					result = Optional.empty();
					break;
				}

				if ((result = sync.getValue()) == null) {
					// 并发时只有第一个线程执行业务逻辑
					E resultObj = null;
					try {
						resultObj = function.get();
					} catch (Throwable e) {
						LoggerUtil.error("execute failed [{}]: {}", key, e.getMessage());
						throw e;
					} finally {
						result = Optional.ofNullable(resultObj);
						if (!result.isPresent()) {
							sync.countError();
						}
						sync.setValue(result);
					}
				}
			} catch (Throwable t) {
				handleThrowable(t);
				LoggerUtil.error(t, "execute error [{}]", key);
			} finally {
				if (result != null) {
					sync.countDown();
				}
			}
		}

		E returnObj = null;
		if (result.isPresent()) {
			returnObj = (E) result.get();
		}

		LoggerUtil.debug("execute success [{}]: {}", key, returnObj);
		return returnObj;
	}

	/*
	 * 同步器
	 */
	@SuppressWarnings("unused")
	private class Sync extends AbstractQueuedSynchronizer {
		private static final long serialVersionUID = 1L;

		// 请求计数器
		private volatile AtomicLong counter;
		// 最近错误次数
		private volatile AtomicLong errorCount;
		// 最大并发数
		private volatile long limit = -1;
		// 最大错误重试次数，超过阀值则后面等待的连接直接快速失败
		private volatile long errorLimit = -1;
		// 最大等待时间，单位：纳秒
		private volatile long timeout = -1;
		// 是否正在执行业务调用
		private volatile boolean running = false;
		// 缓存结果
		private volatile Optional<Object> value;

		public Sync(long limit, long errorLimit, long timeout) {
			this.counter = new AtomicLong(0);
			this.errorCount = new AtomicLong(0);
			this.limit = limit;
			this.errorLimit = errorLimit;
			this.timeout = timeout;

			// 1初始状态；0独占状态；-1共享状态
			setState(1);
		}

		@Override
		protected int tryAcquireShared(int count) {
			if (!running && getState() < 0) {
				// 获取共享锁成功
				return 1;
			}

			if (!running && compareAndSetState(1, 0)) {
				running = true;
				// 获取独占锁成功，进入执行
				return 0;
			}

			// 正有线程独占锁执行业务调用，进行等待
			return -1;
		}

		@Override
		protected boolean tryReleaseShared(int count) {
			if (count == 0) {
				while (!compareAndSetState(-1, 1)) {
					if (getState() >= 0) {
						return true;
					}
				}

				// 最后一个线程清除缓存并重置异常数
				setValue(null);
				errorCount.set(0);
			}
			return true;
		}

		public Optional<Object> getValue() {
			return value;
		}

		public void setValue(Optional<Object> value) {
			if (value == null) {
				// 最后一个线程置空
				this.value = null;
				return;
			}

			if (value.isPresent() || errorCount.get() > errorLimit) {
				// 执行完成（成功或超过最大错误次数），开启共享模式
				setState(-1);
				this.value = value;
			} else {
				// 执行失败且还可重试
				setState(1);
			}
			
			running = false;
		}

		public boolean countUpOrAwait() throws InterruptedException {
			long newCount = counter.incrementAndGet();
			if (newCount > limit || errorCount.get() > errorLimit) {
				return false;
			}

			if (LoggerUtil.isDebugEnabled()) {
				LoggerUtil.debug("Counting up[{}] latch={}", Thread.currentThread().getName(), newCount);
			}

			return tryAcquireSharedNanos((int) newCount, timeout);
		}

		public long countDown() {
			long newCount = counter.decrementAndGet();
			releaseShared((int) newCount);

			if (LoggerUtil.isDebugEnabled()) {
				LoggerUtil.debug("Counting down[{}] latch={}", Thread.currentThread().getName(), newCount);
			}
			return newCount;
		}

		public void countError() {
			errorCount.incrementAndGet();
		}

		public void setLimit(long limit) {
			this.limit = limit;
		}

		public void setErrorLimit(long errorLimit) {
			this.errorLimit = errorLimit;
		}

		public void reset() {
			this.counter.set(0);
			this.errorCount.set(0);
		}

	}

	private static void handleThrowable(Throwable t) {
		if (t instanceof ThreadDeath) {
			throw (ThreadDeath) t;
		}
		if (t instanceof StackOverflowError) {
			return;
		}
		if (t instanceof VirtualMachineError) {
			throw (VirtualMachineError) t;
		}
	}

}
