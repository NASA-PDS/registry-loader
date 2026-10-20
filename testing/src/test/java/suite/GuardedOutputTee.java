package suite;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

final class GuardedOutputTee {
  private static final class Tee extends OutputStream {
    private final OutputStream a;
    private final OutputStream b;
    private final Object lock;
    Tee(OutputStream a, OutputStream b, Object lock) {
        this.a = a;
        this.b = b;
        this.lock = lock;
    }
    @Override
    public void write(int x) throws IOException {
      synchronized (lock) { a.write(x); b.write(x); }
    }
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      synchronized (lock) { a.write(buf, off, len); b.write(buf, off, len); }
    }
    @Override
    public void flush() throws IOException {
      synchronized (lock) { a.flush(); b.flush(); }
    }
  }

  private static final AtomicBoolean ACTIVE = new AtomicBoolean();
  private static final Logger log = LoggerFactory.getLogger(GuardedOutputTee.class);
  private GuardedOutputTee() {}

  static void run(Path logFile, CliAware.Operation op) {
      if (!ACTIVE.compareAndSet(false, true)) {
          throw new IllegalStateException("teeOutputForOperation is not reentrant/parallel-safe. Do not parellelize cucumber.");
      }
      PrintStream origOut = System.out, origErr = System.err;
      try {
          Files.createDirectories(logFile.toAbsolutePath().getParent());
          try (OutputStream file = Files.newOutputStream(logFile, CREATE, TRUNCATE_EXISTING, WRITE)) {
              var lock = new Object();
              System.setOut(new PrintStream(new Tee(origOut, file, lock), true, UTF_8));
              System.setErr(new PrintStream(new Tee(origErr, file, lock), true, UTF_8));
              try {
                  op.run();
              } finally {
                  System.out.flush();
                  System.err.flush();
              }
          }
      } catch (IOException e) {
        log.error("Cannot tee the output to {}. Now in an illegal state to proceed,", logFile, e);
        throw new IllegalStateException("cannot enter the 'tee' state");
      } finally {
          System.setOut(origOut);
          System.setErr(origErr);
          ACTIVE.set(false);
      }
  }
}
