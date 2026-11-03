package cucumber;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class ProxyStream extends PrintStream {
  private static final ThreadLocal<FileOutputStream> currentFileStream = new ThreadLocal<>();
  private final PrintStream originalStream;

  public ProxyStream(PrintStream originalStream) {
    super(originalStream, true); // true enables auto-flush at the proxy level
    this.originalStream = originalStream;
  }

  // This is your "set" attribute mechanism
  public static void routeTo(File logFile) throws IOException {
    // Ensure directories exist
    if (logFile.getParentFile() != null) {
      logFile.getParentFile().mkdirs();
    }
    currentFileStream.set(new FileOutputStream(logFile, false)); // false = truncate/overwrite
  }

  // Clear the attribute when the scenario finishes
  public static void stopRouting() {
    FileOutputStream fos = currentFileStream.get();
    if (fos != null) {
      try {
        fos.flush();
        fos.close();
      } catch (IOException ignored) {
      }
      currentFileStream.remove();
    }
  }

  @Override
  public void write(int b) {
    originalStream.write(b);
    FileOutputStream fos = currentFileStream.get();
    if (fos != null) {
      try {
        fos.write(b);
        fos.flush();
      } catch (IOException ignored) {
      }
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    originalStream.write(buf, off, len);
    FileOutputStream fos = currentFileStream.get();
    if (fos != null) {
      try {
        fos.write(buf, off, len);
        fos.flush();
      } catch (IOException ignored) {
      }
    }
  }

  @Override
  public void flush() {
    originalStream.flush();
    FileOutputStream fos = currentFileStream.get();
    if (fos != null) {
      try {
        fos.flush();
      } catch (IOException ignored) {
        // ignored because it should never happen and what should happen with it
      }
    }
  }
}
