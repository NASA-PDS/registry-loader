package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch._types.mapping.BinaryProperty;
import org.opensearch.client.opensearch._types.mapping.BooleanProperty;
import org.opensearch.client.opensearch._types.mapping.DateProperty;
import org.opensearch.client.opensearch._types.mapping.DoubleNumberProperty;
import org.opensearch.client.opensearch._types.mapping.FloatNumberProperty;
import org.opensearch.client.opensearch._types.mapping.IntegerNumberProperty;
import org.opensearch.client.opensearch._types.mapping.KeywordProperty;
import org.opensearch.client.opensearch._types.mapping.LongNumberProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TextProperty;

final class PropertyHelper {
  static Property.Builder setType (Property.Builder builder, String fieldType) {
    switch (fieldType) {
      case "binary":
        builder.binary(new BinaryProperty.Builder().build());
        break;
      case "boolean":
        builder.boolean_(new BooleanProperty.Builder().build());
        break;
      case "date":
        builder.date(new DateProperty.Builder().build());
        break;
      case "double":
        builder.double_(new DoubleNumberProperty.Builder().build());
        break;
      case "float":
        builder.float_(new FloatNumberProperty.Builder().build());
        break;
      case "integer":
        builder.integer(new IntegerNumberProperty.Builder().build());
        break;
      case "keyword":
        builder.keyword(new KeywordProperty.Builder().build());
        break;
      case "long":
        builder.long_(new LongNumberProperty.Builder().build());
        break;
      case "text":
        builder.text(new TextProperty.Builder().build());
        break;
      default:
        throw new RuntimeException("Cannot map type '" + fieldType + "' yet. Please review PropertyHelper.setType() code and fix.");
    }
    return builder;
  }
}
