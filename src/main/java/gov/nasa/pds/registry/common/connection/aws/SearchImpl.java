package gov.nasa.pds.registry.common.connection.aws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.opensearch.client.opensearch._types.FieldSort;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.TermsAggregation;
import org.opensearch.client.opensearch._types.aggregations.TopHitsAggregation;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.FieldAndFormat;
import org.opensearch.client.opensearch._types.query_dsl.IdsQuery;
import org.opensearch.client.opensearch._types.query_dsl.MatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.Query.Builder;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.search.SourceConfig;
import org.opensearch.client.opensearch.core.search.SourceFilter;
import gov.nasa.pds.registry.common.Request.Search;

class SearchImpl implements Search {
  final SearchRequest.Builder craftsman = new SearchRequest.Builder();
  private void buildIds (Collection<String> lids, boolean alt) {
    SourceConfig.Builder journeyman = new SourceConfig.Builder();
    if (alt) {
      journeyman.filter(new SourceFilter.Builder().includes("alternate_ids").build());
    }
    this.craftsman.query(new Query.Builder().ids(new IdsQuery.Builder().values(new ArrayList<String>(lids)).build()).build());
    this.craftsman.size(lids.size());
    this.craftsman.source(journeyman.fetch(alt).build());
  }
  private Query.Builder matchQuery (String fieldname, String fieldvalue) {
    return (Builder)new Query.Builder().match(new MatchQuery.Builder().field(fieldname).query(new FieldValue.Builder().stringValue(fieldvalue).build()).build());
  }
  @Override
  public Search buildFindDuplicates(int page_size) {
    this.craftsman.aggregations("duplicates",
        new Aggregation.Builder().terms(
            new TermsAggregation.Builder().field("ops:Data_File_Info/ops:file_ref").minDocCount(2).size(page_size).build())
        .build());
    return this;
  }
  @Override
  public Search buildAlternativeIds(Collection<String> lids) {
    this.buildIds(lids, true);
    return this;
  }
  @Override
  public Search buildLatestLidVids(Collection<String> lids) {
    ArrayList<FieldValue> terms = new ArrayList<FieldValue>(lids.size());
    for (String lid : lids) {
      terms.add(new FieldValue.Builder().stringValue(lid).build());
    }
    Aggregation journeyman = new Aggregation.Builder()
        .terms(new TermsAggregation.Builder().field("lid").size(5000).build()).build(); // size is hardcoded in JsonHelper:94
    this.craftsman.aggregations("lids", journeyman);
    journeyman = new Aggregation.Builder()
        .topHits(new TopHitsAggregation.Builder()
            .sort(new SortOptions.Builder()
                .field(new FieldSort.Builder().field("lid").order(SortOrder.Desc).build()).build()).build()).build();
    this.craftsman.aggregations("latest", journeyman);
    this.craftsman.query(new Query.Builder().terms(new TermsQuery.Builder().field("lid").terms(new TermsQueryField.Builder().value(terms).build()).build()).build());
    this.craftsman.size(1);
    this.craftsman.source(new SourceConfig.Builder().fetch(false).build());
    return this;
  }
  @Override
  public Search buildListFields(String dataType) {
    this.craftsman.query(new Query.Builder().bool(new BoolQuery.Builder().must(this.matchQuery("es_data_type", dataType).build()).build()).build());
    this.craftsman.size(1000); // have no idea why hardcoded but it is (.es.JsonHelper:217
    this.craftsman.source(new SourceConfig.Builder().filter(new SourceFilter.Builder().includes("es_field_name").build()).build());
    return this;
  }
  @Override
  public Search buildListLdds(String namespace) {
    BoolQuery.Builder journeyman = new BoolQuery.Builder()
        .must(this.matchQuery("class_ns", "registry").build(),
            this.matchQuery("class_name", "LDD_Info").build(),
            this.matchQuery("attr_ns", namespace).build());
    this.craftsman.query(new Query.Builder().bool(journeyman.build()).build());
    this.craftsman.size(1000); // have no idea why hardcoded but it is (.es.JsonHelper:265
    this.craftsman.source(new SourceConfig.Builder().filter(new SourceFilter.Builder().includes("date", "attr_name").build()).build());
    return this;
  }
  @Override
  public Search buildTheseIds(Collection<String> lids) {
    this.buildIds(lids, false);
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
  @Override
  public Search all(String sortField, int size, String searchAfter) {
    /** opensearch 3.x
    FieldValue fv = new FieldValue.Builder().stringValue(searchAfter).build();
    this.craftsman.searchAfter(Arrays.asList(fv));
    */
    /** opensearch 2.x */
    this.craftsman.searchAfter(Arrays.asList(searchAfter));
    this.craftsman.sort(new SortOptions.Builder()
        .field(new FieldSort.Builder().field(sortField).order(SortOrder.Asc).build())
        .build());
    return this;
  }
  @Override
  public Search all(String filterField, String filterValue, String sortField, int size,
      String searchAfter) {
    this.all(sortField, size, searchAfter);
    this.buildGetField(filterField, filterValue);
    return this;
  }
  @Override
  public Search buildGetField(String field_name, String field_value) {
    BoolQuery.Builder journeyman = new BoolQuery.Builder()
        .filter(new Query.Builder().term(new TermQuery.Builder()
            .field(field_name)
            .value(new FieldValue.Builder().stringValue(field_value).build())
            .build()).build());
    this.craftsman.query(new Query.Builder().bool(journeyman.build()).build());
    return this;
  }
  @Override
  public Search buildLidvidsFromTermQuery(String fieldname, String value) {
    TermQuery.Builder journeyman = new TermQuery.Builder()
        .field(fieldname)
        .value(new FieldValue.Builder().stringValue(value).build());
    this.craftsman.query(new Query.Builder().term(journeyman.build()).build());
    this.craftsman.fields(new FieldAndFormat.Builder().field("lidvid").build());   
    return this;
  }
  @Override
  public Search buildTermQuery(String fieldname, String value) {
    TermQuery.Builder journeyman = new TermQuery.Builder()
        .field(fieldname)
        .value(new FieldValue.Builder().stringValue(value).build());
    this.craftsman.query(new Query.Builder().term(journeyman.build()).build());   
    return this;
  }
  @Override
  public Search buildTermQueryWithoutTermQuery (String yesFieldname, String yesValue, String noFieldname, String noValue) {
    BoolQuery.Builder journeyman = new BoolQuery.Builder()
        .must(new Query.Builder().term(new TermQuery.Builder()
            .field(yesFieldname)
            .value(new FieldValue.Builder().stringValue(yesValue).build()).build()).build())
        .mustNot(new Query.Builder().term(new TermQuery.Builder()
            .field(noFieldname)
            .value(new FieldValue.Builder().stringValue(noValue).build()).build()).build());
    this.craftsman.query(new Query.Builder().bool(journeyman.build()).build());   
    return this;
  }
  @Override
  public Search setReturnedFields(Collection<String> names) {
    this.craftsman.source(new SourceConfig.Builder()
        .filter(new SourceFilter.Builder()
            .includes(new ArrayList<String>(names)).build()).build());
    return this;
  }
}
