package gov.nasa.pds.registry.common.connection.aws;

import java.util.Collection;
import org.opensearch.client.opensearch.core.SearchRequest;
import gov.nasa.pds.registry.common.Request.Search;

public class SearchImpl implements Search {
  final SearchRequest.Builder craftsman = new SearchRequest.Builder();
  @Override
  public Search buildAlternativeIds(Collection<String> lids) {
    return this;
  }
  @Override
  public Search buildLatestLidVids(Collection<String> lids) {
    return this;
  }
  @Override
  public Search buildListFields(String dataType) {
    return this;
  }
  @Override
  public Search buildListLdds(String namespace) {
    return this;
  }
  @Override
  public Search buildTheseIds(Collection<String> lids) {
    return this;
  }
  @Override
  public Search setIndex(String name) {
    this.craftsman.index(name);
    return this;
  }
  @Override
  public Search setPretty(boolean pretty) {
    // ignored because Java v2 returns a document not JSON
    return this;
  }
}
