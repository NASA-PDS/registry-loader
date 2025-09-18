package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import gov.nasa.pds.registry.common.Response;

class CreateIndexRespWrap implements Response.CreatedIndex {
  final private CreateIndexResponse parent;
  CreateIndexRespWrap(CreateIndexResponse parent) {
    this.parent = parent;
  }
  @Override
  public boolean acknowledge() {
    return this.parent.acknowledged();
  }
  @Override
  public boolean acknowledgeShards() {
    return this.parent.shardsAcknowledged();
  }
  @Override
  public String getIndex() {
    return this.parent.index();
  }
}
