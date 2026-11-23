package suite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cucumber.ProxyStream;

final class GuardedOutputTee {
  private static final AtomicBoolean ACTIVE = new AtomicBoolean();
  private static final Logger log = LoggerFactory.getLogger(GuardedOutputTee.class);

  private GuardedOutputTee() {}

  static void run(Path logFile, CliAware.Operation op) {
    if (!ACTIVE.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "teeOutputForOperation is not reentrant/parallel-safe. Do not parellelize cucumber.");
    }
    try {
      ProxyStream.routeTo(logFile.toFile());
      try {
        op.run();
      } finally {
        // Explicitly flush our proxy wrappers to commit any trailing logs
        System.out.flush();
        System.err.flush();
      }
    } catch (IOException e) {
      log.error("Cannot tee the output to {}. Now in an illegal state to proceed,", logFile, e);
      throw new IllegalStateException("cannot enter the 'tee' state");
    } finally {
      // Clear the attribute and safely close the underlying file handle
      ProxyStream.stopRouting();
      ACTIVE.set(false);
    }
  }
}
