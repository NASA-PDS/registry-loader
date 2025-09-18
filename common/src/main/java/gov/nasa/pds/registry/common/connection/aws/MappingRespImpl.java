package gov.nasa.pds.registry.common.connection.aws;

import java.util.HashSet;
import java.util.Set;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;
import gov.nasa.pds.registry.common.Response;

class MappingRespImpl implements Response.Mapping {
  final Set<String> fieldNames = new HashSet<String>();
  MappingRespImpl (GetMappingResponse response) {
    for (IndexMappingRecord idx : response.result().values()) {
      this.fieldNames.addAll(idx.mappings().properties().keySet());
    }
  }
  MappingRespImpl (PutMappingResponse response) {}
  @Override
  public Set<String> fieldNames() {
    return new HashSet<String>(this.fieldNames); // make a copy so data is never corrupted
  }
}
