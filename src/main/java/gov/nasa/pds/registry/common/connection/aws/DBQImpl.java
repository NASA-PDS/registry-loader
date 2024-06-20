package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import gov.nasa.pds.registry.common.Request;

class DBQImpl implements Request.DeleteByQuery {
  final DeleteByQueryRequest.Builder craftsman = new DeleteByQueryRequest.Builder();
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
    return this;
  }
  @Override
  public Request.DeleteByQuery setRefresh(boolean state) {
    this.craftsman.refresh(state);
    return this;
  }
}
