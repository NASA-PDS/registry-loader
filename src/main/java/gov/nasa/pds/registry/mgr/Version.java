package gov.nasa.pds.registry.mgr;

public class Version extends gov.nasa.pds.registry.common.Version {
  private class SubVersion extends Version {
    private final String name;
    SubVersion(String cmdname) {
      this.name = Version.this.getName() + "-" + cmdname;
    }
    @Override
    public String getName() {
      return this.name;
    }
    @Override
    public String toString() {
      return Version.this.toString();
    }
    @Override
    public Semantic value() {
      return Version.this.value();
    }
  }
  private static Version self = null;
  public static synchronized Version instance() {
    if (self == null) {
      self = new Version();
    }
    return self;
  }
  @Override
  public String getName() {
    return "registry-manager";
  }
  public Version subcommand (String cmdname) {
    return new SubVersion(cmdname);
  }
}
