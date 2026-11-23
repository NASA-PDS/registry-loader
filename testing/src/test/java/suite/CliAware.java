package suite;

import java.nio.file.Path;

public interface CliAware extends Runnable {
  @FunctionalInterface
  interface Operation {
      void run();
  }
  static void teeOutputForOperation(Path logFile, Operation op) {
      GuardedOutputTee.run(logFile, op);
  }

  public void arguments(String args);
  public void logpath(Path path);
}
