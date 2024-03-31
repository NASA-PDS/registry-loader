package gov.nasa.pds.registry.common.connection.es;

import java.util.Collection;
import gov.nasa.pds.registry.common.Request.Search;

class SearchImpl implements Search {
  private boolean pretty = false;
  private String index;
  String json = null;
  @Override
  public Search buildAlternativeIds(Collection<String> lids) {
    JsonHelper.buildSearchIdsRequest(lids, lids.size(), true);
    return this;
  }
  @Override
  public Search buildLatestLidVids(Collection<String> lids) {
    this.json = JsonHelper.buildGetLatestLidVidsJson(lids);
    return this;
  }
  @Override
  public Search buildListFields(String dataType) {
    this.json = JsonHelper.buildListFieldsRequest(dataType);
    return this;
  }
  @Override
  public Search buildListLdds(String namespace) {
    this.json = JsonHelper.buildListLddsRequest(namespace);
    return this;
  }
  @Override
  public Search buildTheseIds(Collection<String> lids) {
    JsonHelper.buildSearchIdsRequest(lids, lids.size(), true);
    return this;
  }
  @Override
  public Search setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + "/_search" + (this.pretty ? "?pretty" : "");
  }
  @Override
  public Search setPretty(boolean pretty) {
    this.pretty = pretty;
    return this;
  }
}
