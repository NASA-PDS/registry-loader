package gov.nasa.pds.registry.common.connection.es;

import gov.nasa.pds.registry.common.Request.Count;

class CountImpl implements Count {
  private String index;
  private String query;
  @Override
  public Count setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public Count setQuery(String q) {
    this.query = q;
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + "/_count?q=" + this.query;
  }
}
