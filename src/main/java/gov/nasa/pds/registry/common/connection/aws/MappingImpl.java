package gov.nasa.pds.registry.common.connection.aws;

import java.util.Collection;
import java.util.HashMap;
import org.opensearch.client.opensearch._types.mapping.BooleanProperty;
import org.opensearch.client.opensearch._types.mapping.DoubleNumberProperty;
import org.opensearch.client.opensearch._types.mapping.FloatNumberProperty;
import org.opensearch.client.opensearch._types.mapping.IntegerNumberProperty;
import org.opensearch.client.opensearch._types.mapping.KeywordProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;
import org.opensearch.client.opensearch.indices.GetMappingRequest;
import org.opensearch.client.opensearch.indices.PutMappingRequest;
import gov.nasa.pds.registry.common.Request.Mapping;
import gov.nasa.pds.registry.common.util.Tuple;

public class MappingImpl implements Mapping {
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
      switch (fieldType) {
        case "boolean":
          journeyman.boolean_(new BooleanProperty.Builder().build());
          break;
        case "double":
          journeyman.double_(new DoubleNumberProperty.Builder().build());
          break;
        case "float":
          journeyman.float_(new FloatNumberProperty.Builder().build());
          break;
        case "integer":
          journeyman.integer(new IntegerNumberProperty.Builder().build());
          break;
        case "keyword":
          journeyman.keyword(new KeywordProperty.Builder().build());
          break;
        case "text":
          journeyman.text(new TextProperty.Builder().build());
          break;
        default:
          throw new RuntimeException("Cannot map type '" + fieldType + "' yet. Please review code and fix.");
      }
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
