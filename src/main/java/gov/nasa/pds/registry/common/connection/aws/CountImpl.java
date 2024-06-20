package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.core.CountRequest;
import gov.nasa.pds.registry.common.Request.Count;

class CountImpl implements Count {
  final CountRequest.Builder craftsman = new CountRequest.Builder();
  @Override
  public Count setIndex(String name) {
    this.craftsman.index(name);
    return this;
  }
  @Override
  public Count setQuery(String q) {
    this.craftsman.q(q);
    return this;
  }
}
