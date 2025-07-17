package gov.nasa.pds.registry.common;

import java.io.InputStream;
import java.util.Properties;

public class Version {
  public final String VERSION;
  private Version() {
    Properties prop = new Properties();
    String version = "1000000.100000.100000";
    try (InputStream input = Version.class.getClassLoader().getResourceAsStream("version.property")) {
        if (input != null) {
          prop.load(input);
          version = prop.getProperty("application.version");
        }
    } catch (Exception ex) {
        ex.printStackTrace();
    } finally {
      VERSION = version;
    }    
  }
  private static Version self = null;
  public static synchronized String asString() {
    if (self == null) {
      self = new Version();
    }
    return self.VERSION;
  }
}
