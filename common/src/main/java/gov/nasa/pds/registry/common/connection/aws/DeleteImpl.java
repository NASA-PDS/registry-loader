package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.core.DeleteRequest;
import gov.nasa.pds.registry.common.Request.Delete;

class DeleteImpl implements Delete {
  final DeleteRequest.Builder craftsman = new DeleteRequest.Builder();
  @Override
  public Delete setDocId(String id) {
    this.craftsman.id(id);
    return this;
  }
  @Override
  public Delete setIndex(String name) {
    this.craftsman.index(name);
    return this;
  }
}
