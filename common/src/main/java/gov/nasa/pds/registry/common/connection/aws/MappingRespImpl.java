package gov.nasa.pds.registry.common.connection.aws;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;
import gov.nasa.pds.registry.common.Response;

class MappingRespImpl implements Response.Mapping {
  final Set<String> fieldNames = new HashSet<String>();
  MappingRespImpl (GetMappingResponse response) {
    for (IndexMappingRecord idx : response.result().values()) {
      for (Map.Entry<String,Property> field : idx.mappings().properties().entrySet()) {
        if (field.getValue().isObject()) {
          this.fieldNames.addAll(unravel(field.getValue().object().properties(), field.getKey()));
        } else {
          this.fieldNames.add(field.getKey());
        }
      }
    }
  }
  private Collection<String> unravel(Map<String,Property> properties, String heritage) {
    HashSet<String> fields = new HashSet<String>();
    heritage += ".";
    for (Map.Entry<String,Property> field : properties.entrySet()) {
      if (field.getValue().isObject()) {
        fields.addAll(unravel(field.getValue().object().properties(), heritage + field.getKey()));
      } else {
        fields.add(heritage + field.getKey());
      }
    }
    return fields;
  }
  MappingRespImpl (PutMappingResponse response) {}
  @Override
  public Set<String> fieldNames() {
    return new HashSet<String>(this.fieldNames); // make a copy so data is never corrupted
  }
}
