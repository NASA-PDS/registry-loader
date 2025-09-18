package gov.nasa.pds.registry.common.connection.aws;

import java.util.Collection;
import java.util.HashMap;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.indices.GetMappingRequest;
import org.opensearch.client.opensearch.indices.PutMappingRequest;
import gov.nasa.pds.registry.common.Request.Mapping;
import gov.nasa.pds.registry.common.util.Tuple;

class MappingImpl implements Mapping {
  boolean isGet = true;
  final GetMappingRequest.Builder craftsman_get = new GetMappingRequest.Builder();
  final PutMappingRequest.Builder craftsman_set = new PutMappingRequest.Builder();
  @Override
  public Mapping buildUpdateFieldSchema(Collection<Tuple> pairs) {
    HashMap<String,Property> mapping = new HashMap<String,Property>();
    for (Tuple t : pairs) {
      Property.Builder journeyman = new Property.Builder();
      String fieldName = t.item1;
      String fieldType = t.item2;
      PropertyHelper.setType(journeyman, fieldType);
      mapping.put(fieldName, journeyman.build());
    }
    this.craftsman_set.properties(mapping);
    this.isGet = false;
    return this;
  }

  @Override
  public Mapping setIndex(String name) {
    this.craftsman_get.index(name);
    this.craftsman_set.index(name);
    return this;
  }

}
