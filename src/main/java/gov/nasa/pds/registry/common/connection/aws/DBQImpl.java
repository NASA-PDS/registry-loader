package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.SearchRequest;
import gov.nasa.pds.registry.common.Request;

class DBQImpl implements Request.DeleteByQuery {
  final SearchRequest.Builder craftsman = new SearchRequest.Builder().scroll(new Time.Builder().time("24m").build());
  String index;
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
    this.index = name;
    return this;
  }
  @Override
  public Request.DeleteByQuery setRefresh(boolean state) {
    // AOSS has not refresh concept
    return this;
  }
}
