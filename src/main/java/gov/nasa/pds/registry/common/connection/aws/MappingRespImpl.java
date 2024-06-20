package gov.nasa.pds.registry.common.connection.aws;

import java.util.HashSet;
import java.util.Set;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import gov.nasa.pds.registry.common.Response;

class MappingRespImpl implements Response.Mapping {
  final Set<String> fieldNames;
  MappingRespImpl (GetMappingResponse response) {
    this.fieldNames = response.result().keySet();
  }
  MappingRespImpl (PutMappingResponse response) {
    this.fieldNames = new HashSet<String>();
  }
  @Override
  public Set<String> fieldNames() {
    return new HashSet<String>(this.fieldNames); // make a copy so data is never corrupted
  }
}
