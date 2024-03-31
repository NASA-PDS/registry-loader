package gov.nasa.pds.registry.common.connection.es;

import java.util.Collection;
import gov.nasa.pds.registry.common.Request.Mapping;
import gov.nasa.pds.registry.common.util.Tuple;

class MappingImpl implements Mapping {
  private String index;
  String json = null;
  @Override
  public Mapping buildUpdateFieldSchema(Collection<Tuple> pairs) {
    this.json = JsonHelper.buildUpdateSchemaRequest(pairs);
    return this;
  }
  @Override
  public Mapping setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + (this.json == null ? "/_mappings" : "/_mapping");
  }
}
