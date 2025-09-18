package gov.nasa.pds.registry.mgr.cmd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Known {
  private static Known self = null;
  private final List<String> commands;
  private Known(List<String> commands) {
    this.commands = commands;
  }
  public static synchronized List<String> get() {
    if (self == null) {
      throw new IllegalStateException("Called get() before set().");
    }
    return new ArrayList<String>(self.commands);
  }
  public static synchronized void set(Collection<String> commands) {
    if (self == null) {
      self = new Known(new ArrayList<String>(commands));
    } else {
      throw new IllegalStateException("The set can only be called once.");
    }
  }
}
