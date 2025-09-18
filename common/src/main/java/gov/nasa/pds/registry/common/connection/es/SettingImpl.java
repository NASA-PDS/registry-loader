package gov.nasa.pds.registry.common.connection.es;

import gov.nasa.pds.registry.common.Request.Setting;

class SettingImpl implements Setting {
  private String index;
  @Override
  public Setting setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + "/_settings";
  }
}
