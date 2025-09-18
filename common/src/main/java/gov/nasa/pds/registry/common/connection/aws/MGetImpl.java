package gov.nasa.pds.registry.common.connection.aws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.opensearch.client.opensearch.core.MgetRequest;
import gov.nasa.pds.registry.common.Request.Get;
import gov.nasa.pds.registry.common.Request.MGet;

class MGetImpl implements MGet {
  final MgetRequest.Builder craftsman = new MgetRequest.Builder();
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
    this.craftsman.ids(id);
    return this;
  }
  @Override
  public Get setIndex(String index) {
    this.craftsman.index(index);
    return this;
  }
  @Override
  public MGet setIds(Collection<String> ids) {
    this.craftsman.ids(new ArrayList<String>(ids));
    return this;
  }
}
