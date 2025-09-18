package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
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
  public Count setQuery(String collectionLidvid, String refType) {
    this.craftsman.query (new Query.Builder().bool(new BoolQuery.Builder().must(
        new Query.Builder().term(new TermQuery.Builder()
            .field("collection_lidvid")
            .value(new FieldValue.Builder().stringValue(collectionLidvid).build())
            .build()).build(),
        new Query.Builder().term(new TermQuery.Builder()
            .field("reference_type")
            .value(new FieldValue.Builder().stringValue(refType).build())
            .build()).build())
        .build()).build());
    return this;
  }
}
