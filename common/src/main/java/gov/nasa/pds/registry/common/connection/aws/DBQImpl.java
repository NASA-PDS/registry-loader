package gov.nasa.pds.registry.common.connection.aws;

import java.util.ArrayList;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.search.SourceConfig;
import org.opensearch.client.opensearch.core.search.SourceFilter;
import gov.nasa.pds.registry.common.Request;

class DBQImpl implements Request.DeleteByQuery {
  final SearchRequest.Builder craftsman = new SearchRequest.Builder()
      .source(new SourceConfig.Builder().filter(new SourceFilter.Builder().includes("lidvid").build()).build());
  final ArrayList<String> index = new ArrayList<String>();
  @Override
  public Request.DeleteByQuery createFilterQuery(String key, String value) {
    this.craftsman.query(new Query.Builder().term(new TermQuery.Builder()
        .field(key)
        .value(new FieldValue.Builder().stringValue(value).build())
        .build()).build());
    return this;
  }
  @Override
  public Request.DeleteByQuery createMatchAllQuery() {
    this.craftsman.query(new Query.Builder().build());
    return this;
  }
  @Override
  public Request.DeleteByQuery setIndex(String name) {
    this.craftsman.index(name);
    this.index.add(name);
    return this;
  }
  @Override
  public Request.DeleteByQuery setRefresh(boolean state) {
    // AOSS has not refresh concept
    return this;
  }
}
