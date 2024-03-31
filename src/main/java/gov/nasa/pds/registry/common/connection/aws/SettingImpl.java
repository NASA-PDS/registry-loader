package gov.nasa.pds.registry.common.connection.aws;

import gov.nasa.pds.registry.common.Request.Setting;

public class SettingImpl implements Setting {
  @Override
  public Setting setIndex(String name) {
    return this;
  }
}
