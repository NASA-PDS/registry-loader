package gov.nasa.pds.registry.common.connection.es;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import gov.nasa.pds.registry.common.Request.Get;
import gov.nasa.pds.registry.common.Request.MGet;

class GetImpl implements MGet {
  final private ArrayList<String> excludes = new ArrayList<String>();
  final private ArrayList<String> includes = new ArrayList<String>();
  private String id = null;
  private String index;
  String json = null;
  @Override
  public Get excludeField(String field) {
    this.excludes.add(field);
    return this;
  }
  @Override
  public Get excludeFields(List<String> fields) {
    this.excludes.addAll(fields);
    return this;
  }
  @Override
  public Get includeField(String field) {
    this.includes.add(field);
    return this;
  }
  @Override
  public Get includeFields(List<String> fields) {
    this.includes.addAll(fields);
    return this;
  }
  @Override
  public Get setId(String id) {
    this.setId(id);
    return this;
  }
  @Override
  public MGet setIds(Collection<String> ids) {
    this.json = JsonHelper.buildIdList(ids);
    return this;
  }
  @Override
  public Get setIndex(String index) {
    this.setIndex(index);
    return this;
  }
  @Override
  public String toString() {
    String constraints = "";
    if (!this.excludes.isEmpty() || !this.includes.isEmpty()) {
      constraints += "?_source=";
      for (String field : this.includes) {
        constraints += field + ",";
      }
      constraints = constraints.substring(0, constraints.length()-1);
    }
    return "/" + this.index + "/_doc" + (this.id == null ? "" : "/" + this.id) + constraints;
  }
}
