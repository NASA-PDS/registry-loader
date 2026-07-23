package gov.nasa.pds.registry.common.es.service;

import java.io.IOException;
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.dd.LddException;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;

/**
 * Reusable polling helper for search-index propagation delays. Search indexes
 * with eventual-consistency semantics (e.g. OpenSearch Serverless) may not make documents
 * written via bulk load immediately visible to subsequent search or mget calls. Both methods
 * here poll every second up to a caller-specified timeout, then either return the last result
 * or throw.
 */
final class SearchIndexWait {

  static final int DEFAULT_WAIT_SECONDS = 30;

  private SearchIndexWait() {}

  @FunctionalInterface
  interface ThrowingSupplier<T> {
    T get() throws IOException, DataTypeNotFoundException;
  }

  /**
   * Polls {@code op} every second until {@code ready} returns true, or {@code maxSeconds} elapses.
   * Returns the last result regardless of whether the predicate was satisfied; the caller must
   * check the result (e.g. {@code isEmpty()}) and handle the timeout case.
   *
   * @throws LddException              if the thread is interrupted while sleeping
   * @throws IOException               if {@code op} itself throws
   * @throws DataTypeNotFoundException if {@code op} throws it
   */
  static <T> T untilReady(int maxSeconds, ThrowingSupplier<T> op, Predicate<T> ready,
      Logger log, String desc) throws IOException, DataTypeNotFoundException, LddException {
    if (maxSeconds < 0) throw new IllegalArgumentException("maxSeconds must be >= 0, got: " + maxSeconds);
    T result = op.get();
    for (int elapsed = 0; !ready.test(result) && elapsed < maxSeconds; elapsed++) {
      log.debug("{} not yet ready, retrying ({}/{} s)...", desc, elapsed + 1, maxSeconds);
      sleep(desc);
      result = op.get();
    }
    return result;
  }

  /**
   * Polls {@code op} every second, retrying only on {@link DataTypeNotFoundException}, up to
   * {@code maxSeconds}. Returns the result on success. On timeout, throws the last
   * {@code DataTypeNotFoundException}. Any other exception propagates immediately.
   *
   * @throws DataTypeNotFoundException if {@code op} still throws after {@code maxSeconds} seconds
   * @throws LddException              if the thread is interrupted while sleeping
   * @throws IOException               if {@code op} throws an IOException
   */
  static <T> T untilVisible(int maxSeconds, ThrowingSupplier<T> op,
      Logger log, String desc) throws IOException, DataTypeNotFoundException, LddException {
    if (maxSeconds < 0) throw new IllegalArgumentException("maxSeconds must be >= 0, got: " + maxSeconds);
    DataTypeNotFoundException last = null;
    for (int elapsed = 0; elapsed <= maxSeconds; elapsed++) {
      try {
        return op.get();
      } catch (DataTypeNotFoundException e) {
        last = e;
        if (elapsed < maxSeconds) {
          log.debug("{} not yet visible, retrying ({}/{} s)...", desc, elapsed + 1, maxSeconds);
          sleep(desc);
        }
      }
    }
    throw last;
  }

  private static void sleep(String desc) throws LddException {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new LddException("Interrupted while waiting for search index propagation: " + desc, ie);
    }
  }
}
