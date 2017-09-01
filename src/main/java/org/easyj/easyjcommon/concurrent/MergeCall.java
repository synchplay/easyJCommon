package org.easyj.easyjcommon.concurrent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.easyj.easyjlog.util.LoggerUtil;

/**
 * 合并调用 <br>
 * 
 * <br>
 * 使用示例：
 * <code>
 * MergeCall.execute("key", () -> {
 *     return ref.invoke();
 * });
 * </code>
 * 
 * @author dengjianjun
 *
 */
public class MergeCall {

	// 最大循环次数
	private static int MAX_CYCLE = 1000;

	// 超时时间（毫秒）
	private static long MAX_TIMEOUT = 3 * 1000;

	// 最大并发请求数，超过部分快速失败
	private static int MAX_REQUEST = 10000;

	// 睡眠等待时间（毫秒）
	private static long SLEEP_TIME = 10;

	// 定义为 volatile 是为了在 double-check 的时候禁止指令重排
	private static volatile Map<String, AtomicInteger> counter = new ConcurrentHashMap<>();
	private static volatile Map<String, Optional<Object>> cache = new ConcurrentHashMap<>();

	@SuppressWarnings("unchecked")
	public static <E extends Object> E execute(String key, Supplier<E> function) {
		if (key == null || key.trim().length() == 0) {
			return null;
		}

		// 记录开始时间，用于超时处理
		long startTime = System.currentTimeMillis();
		// 记录自旋次数
		int cycleCount = 0;

		AtomicInteger running = counter.get(key);
		if (running == null) {
			// 初始对应的计数器
			synchronized (counter) {
				running = counter.get(key);
				if (running == null) {
					running = new AtomicInteger(0);
					counter.put(key, running);
				}
			}
		}

		Optional<Object> result = null;
		int cur = running.incrementAndGet();

		while (result == null) {
			cycleCount++;

			try {
				if (cur == 1) {
					// 并发时只有第一个线程执行业务逻辑
					E resultObj = null;
					try {
						resultObj = function.get();
					} catch (Exception e) {
						LoggerUtil.error("execute failed [{}]({}ms {}num): {}", key,
								(System.currentTimeMillis() - startTime), cycleCount, e.getMessage());
						throw e;
					} finally {
						result = Optional.ofNullable(resultObj);
						cache.put(key, result);
					}

				} else {
					// 其它线程直接从缓存获取
					result = cache.get(key);
					if (result == null) {
						if (isBreak(key, cur, cycleCount, startTime)) {
							// 超时结束
							result = Optional.empty();
						} else {
							// 睡眠一段时间再次重试
							try {
								Thread.sleep(SLEEP_TIME);
							} catch (InterruptedException e) {
								LoggerUtil.warn(e, "线程等待被中断");
							}
						}
					}
				}
			} finally {
				if (result != null) {
					cur = running.decrementAndGet();
					if (cur == 0) {
						// 最后一个线程清除缓存，考虑到业务逻辑耗时相对较长，所以删除操作并未与插入操作做同步
						cache.remove(key);
					}
				}
			}
		}

		E returnObj = null;
		if (result.isPresent()) {
			returnObj = (E) result.get();
		}

		LoggerUtil.debug("execute success [{}]({}ms {}num): {}", key, (System.currentTimeMillis() - startTime),
				cycleCount, returnObj);
		return returnObj;
	}

	/*
	 * 判断是否超出等待条件
	 */
	private static boolean isBreak(String key, int cur, int cycleCount, long startTime) {
		boolean flag = false;
		if (cur > MAX_REQUEST) {
			flag = true;
		} else if ((System.currentTimeMillis() - startTime) > MAX_TIMEOUT) {
			flag = true;
		} else if (cycleCount > MAX_CYCLE) {
			flag = true;
		}

		if (flag) {
			LoggerUtil.warn("execute break [{}]({}ms {}num): {}", key, (System.currentTimeMillis() - startTime),
					cycleCount, cur);
		}

		return flag;
	}

}
