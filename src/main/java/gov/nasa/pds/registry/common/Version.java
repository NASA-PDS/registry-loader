package gov.nasa.pds.registry.common;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Version {
  public class Semantic {
    public final int major;
    public final int minor;
    public final int patch;
    public Semantic (int major, int minor, int patch) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
    }
    public final boolean greater_than_or_equal_to (Semantic minimum) {
      boolean result = this.major > minimum.major;
      if (!result && this.major == minimum.major) {
        result = this.major > minimum.minor;
        if (!result && this.minor == minimum.minor) {
          result = this.patch >= minimum.patch;
        }
      }
      return result;
    }
    @Override
    public String toString() {
      return String.join(".", Arrays.asList(
          Integer.toString(this.major),
          Integer.toString(this.minor),
          Integer.toString(this.patch)
          ));
    }
  }
  public final String VERSION;
  protected Version() {
    Properties prop = new Properties();
    String version = "1000000.100000.100000";
    try (InputStream input = Version.class.getClassLoader().getResourceAsStream(this.getName() + ".version")) {
        if (input != null) {
          prop.load(input);
          version = prop.getProperty("application.version");
        }
    } catch (Exception ex) {
      //ex.printStackTrace();
      System.err.println("[ERROR] Internal error that requires a developer to debug: " + ex.getMessage());
    } finally {
      VERSION = version;
    }    
  }
  private static Version self = null;
  public static synchronized Version instance() {
    if (self == null) {
      self = new Version();
    }
    return self;
  }
  protected String getName() {
    return "registry-common";
  }
  public boolean check(Semantic needed) {
    Semantic current = this.value();
    return current.greater_than_or_equal_to (needed);
  }
  public Semantic value() {
    String version = this.VERSION.contains("-") ?
      version = this.VERSION.split("-")[0] : this.VERSION;
    String parts[] = version.split("\\.");
    return new Semantic(
        Integer.valueOf(parts[0]),
        Integer.valueOf(parts[1]),
        Integer.valueOf(parts[2]));
  }
  public Semantic value (Map<String,Integer> content) {
    return new Semantic(content.get("major"),content.get("minor"),content.get("patch"));
  }
  @Override
  public String toString() {
    return this.VERSION;
  }
}
