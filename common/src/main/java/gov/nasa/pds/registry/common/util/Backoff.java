package gov.nasa.pds.registry.common.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter for retrying transient failures.
 *
 * <p>The delay before attempt {@code n} (0-based) is {@code base * 2^n}, capped at {@code max}.
 * Jitter subtracts a random fraction of the delay, so the result is always in
 * {@code [delay * (1 - jitter), delay]} and never exceeds {@code max}.
 */
public final class Backoff {
  private static final int MAX_SHIFT = 30;

  private final long baseMillis;
  private final long maxMillis;
  private final double jitter;

  /**
   * @param baseMillis delay before the first retry (must be &gt; 0)
   * @param maxMillis upper bound for any delay (must be &gt;= baseMillis)
   * @param jitter fraction of the delay, in [0, 1], that may be randomly subtracted
   */
  public Backoff(long baseMillis, long maxMillis, double jitter) {
    if (baseMillis <= 0) throw new IllegalArgumentException("baseMillis should be > 0");
    if (maxMillis < baseMillis) throw new IllegalArgumentException("maxMillis should be >= baseMillis");
    if (jitter < 0.0 || jitter > 1.0) throw new IllegalArgumentException("jitter should be in [0, 1]");
    this.baseMillis = baseMillis;
    this.maxMillis = maxMillis;
    this.jitter = jitter;
  }

  /**
   * Delay before the given retry attempt, with random jitter applied.
   * @param attempt 0-based retry number (0 is the delay before the first retry)
   */
  public long delayMillis(int attempt) {
    return delayMillis(attempt, ThreadLocalRandom.current().nextDouble());
  }

  /**
   * Delay before the given retry attempt using a caller-supplied random value (for tests).
   * @param attempt 0-based retry number
   * @param random value in [0, 1) selecting how much jitter to subtract
   */
  public long delayMillis(int attempt, double random) {
    int shift = Math.max(0, Math.min(attempt, MAX_SHIFT));
    long delay = baseMillis << shift;
    // The shift overflows a long only if baseMillis is huge; treat overflow as "at the cap".
    if (delay < baseMillis || delay > maxMillis) delay = maxMillis;
    return delay - (long) (delay * jitter * random);
  }

  /**
   * Sleep for the delay of the given retry attempt.
   * @param attempt 0-based retry number
   * @return true if the full delay elapsed; false if the thread was interrupted, in which case
   * the interrupt flag is restored and the caller should stop retrying.
   */
  public boolean sleep(int attempt) {
    return pause(delayMillis(attempt));
  }

  /**
   * Sleep for the given number of milliseconds.
   * @return true if the full delay elapsed; false if the thread was interrupted, in which case
   * the interrupt flag is restored and the caller should stop retrying.
   */
  public static boolean pause(long millis) {
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
