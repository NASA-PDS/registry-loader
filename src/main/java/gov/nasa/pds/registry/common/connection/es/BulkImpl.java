package gov.nasa.pds.registry.common.connection.es;


import java.util.Collection;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Request.Bulk;

class BulkImpl implements Request.Bulk {
  private String index = null;
  private String refresh = null;
  String json = "";
  //@Override
  public void add(String statement) {
    this.json += statement + "\n";
  }
  @Override
  public void add(String statement, String document) {
    this.json += statement + "\n";
    this.json += document + "\n";
  }
  @Override
  public Request.Bulk buildUpdateStatus(Collection<String> lidvids, String status) {
    this.json = JsonHelper.buildUpdateStatusJson(lidvids, status);
    return this;
  }
  @Override
  public Bulk setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public Bulk setRefresh(String type) {
    this.refresh = type;
    return this;
  }
  @Override
  public String toString() {
    return (this.index == null ? "" : "/" + this.index) + "/_bulk" + (this.refresh == null ? "" : "?refresh=" + this.refresh);
  }
}
