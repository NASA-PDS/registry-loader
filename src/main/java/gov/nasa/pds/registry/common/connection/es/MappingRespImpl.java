package gov.nasa.pds.registry.common.connection.es;

import java.util.HashSet;
import java.util.Set;
import gov.nasa.pds.registry.common.Response;

class MappingRespImpl implements Response.Mapping {
  final private Set<String> fieldNames;
  MappingRespImpl (org.elasticsearch.client.Response response, String index) {
    this.fieldNames = new MappingsParser(index).parse(response.getEntity());
  }
  @Override
  public Set<String> fieldNames() {
    return new HashSet<String>(fieldNames);
  }

}
