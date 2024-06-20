package gov.nasa.pds.registry.common.connection.aws;

import java.util.List;
import org.opensearch.client.opensearch.core.GetRequest;
import gov.nasa.pds.registry.common.Request.Get;

class GetImpl implements Get {
  final GetRequest.Builder craftsman = new GetRequest.Builder();
  @Override
  public Get excludeField(String field) {
    this.craftsman.sourceExcludes(field);
    return this;
  }
  @Override
  public Get excludeFields(List<String> fields) {
    this.craftsman.sourceExcludes(fields);
    return this;
  }
  @Override
  public Get includeField(String field) {
    this.craftsman.sourceIncludes(field);
    return this;
  }
  @Override
  public Get includeFields(List<String> fields) {
    this.craftsman.sourceIncludes(fields);
    return this;
  }
  @Override
  public Get setId(String id) {
    this.craftsman.id(id);
    return this;
  }
  @Override
  public Get setIndex(String index) {
    this.craftsman.index(index);
    return this;
  }
}
