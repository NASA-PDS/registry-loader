package gov.nasa.pds.registry.common.connection.es;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import gov.nasa.pds.registry.common.Request.Count;

class CountImpl implements Count {
  private String index;
  private String query;
  @Override
  public Count setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public Count setQuery(String collectionLidvid, String refType) throws UnsupportedEncodingException {
    // Elasticsearch "Lucene" query
    String query = "collection_lidvid:\"" + collectionLidvid + "\" AND reference_type:" + refType;
    this.query = URLEncoder.encode(query, "UTF-8");
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + "/_count?q=" + this.query;
  }
}
