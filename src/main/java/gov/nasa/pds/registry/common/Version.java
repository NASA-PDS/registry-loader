package gov.nasa.pds.registry.common;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    public final boolean greaterThanOrEqualTo (Semantic minimum) {
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
  private final Logger log;
  public final String version;
  protected Version() {
    Properties prop = new Properties();
    String v = "1000000.100000.100000";
    this.log = LogManager.getLogger(this.getClass());
    try (InputStream input = Version.class.getClassLoader().getResourceAsStream(this.getName() + ".version")) {
        if (input != null) {
          prop.load(input);
          v = prop.getProperty("application.version");
        }
    } catch (Exception ex) {
      //ex.printStackTrace();
      this.log.error("Internal error that requires a developer to debug: " + ex.getMessage());
    } finally {
      this.version = v;
    }    
  }
  private static Version self = null;
  public static synchronized Version instance() {
    if (self == null) {
      self = new Version();
    }
    return self;
  }
  public String getName() {
    return "registry-common";
  }
  public boolean check(Semantic needed) {
    Semantic current = this.value();
    return current.greaterThanOrEqualTo (needed);
  }
  public final boolean checkVersion(ConnectionFactory conFact, Collection<? extends Version> toolChain) throws Exception {
    // check the database for correct versions
    // print if not the correct version and to upgrade to latest tool
    boolean proceed = true;
    HashMap<String,Boolean> found = new HashMap<String,Boolean>();
    HashMap<String,Version> toolMap = new HashMap<String,Version>();
    for (Version v : toolChain) {
      found.put(v.getName(), Boolean.FALSE);
      toolMap.put(v.getName(), v);
    }
    RestClient client = conFact.setIndexName(conFact.getIndexName() + "-versions").createRestClient();
    Request.Search fetchRequiredVersions = client.createSearchRequest()
        .buildTheseIds(toolMap.keySet())
        .setReturnedFields(Arrays.asList("tool.name", "tool.version.major","tool.version.minor","tool.version.patch"));
    for (Map<String,Object> document : client.performRequest(fetchRequiredVersions).documents()) {
      @SuppressWarnings("unchecked")
      Map<String,Object> tool = (Map<String,Object>)document.get("tool");
      @SuppressWarnings("unchecked")
      Semantic needed = Version.instance().value((Map<String,Integer>)tool.get("version"));
      String name = tool.get("name").toString();
      found.put(name, Boolean.TRUE);
      boolean ok = toolMap.get(name).check(needed);
      if (!ok) {
        this.log.error("Your version of \"{}\" needs to be updated because your version {} is less than {}",
            name, toolMap.get(name).value(), needed);
      }
      proceed &= ok;
    }
    for (Map.Entry<String,Boolean> item : found.entrySet()) {
      if (!item.getValue().booleanValue()) {
        this.log.error("The tool \"{}\" is not registered with this registry and cannot be used.", item.getKey());
        proceed = false;
      }
    }
    client.close();
    return proceed;
  }

  public Semantic value() {
    String[] parts = (this.version.contains("-") ? this.version.split("-")[0] : this.version)
        .split("\\.");
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
    return this.version;
  }
}
