package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.Buckets;
import org.opensearch.client.opensearch._types.aggregations.StringTermsAggregate;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;
import org.opensearch.client.opensearch._types.aggregations.TopHitsAggregate;
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.ScrollResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.es.dao.dd.LddInfo;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;

@SuppressWarnings("unchecked") // JSON heterogenous structures requires raw casting
class SearchRespWrap implements Response.Search {
  final private OpenSearchClient client;
  final private SearchResponse<Object> parent;
  SearchRespWrap(OpenSearchClient client, SearchResponse<Object> parent) {
    this.client = client;
    this.parent = parent;
  }
  @Override
  public Map<String, Set<String>> altIds() throws UnsupportedOperationException, IOException {
    HashMap<String, Set<String>> results = new HashMap<String, Set<String>>();
    if (true) throw new NotImplementedException("Need to fill this out when have a return value");
    return results;
  }
  @Override
  public Set<String> fields() throws UnsupportedOperationException, IOException {
    Set<String> results = new HashSet<String>();
    for (Hit<Object> hit : this.parent.hits().hits()) {
      for (String value : ((Map<String,String>)hit.source()).values()) {
        results.add(value);
      }
    }
    return results;
  }
  @Override
  public List<String> lidvids() {
    ArrayList<String> lidvids = new ArrayList<String>();
    String scrollID = this.parent.scrollId();
    for (Hit<Object> hit : this.parent.hits().hits()) {
      lidvids.add(((Map<String,String>)hit.source()).get("lidvid"));
    }
    if (this.parent.scrollId() != null) {
      try {
        ScrollResponse<Object> page;
        while (lidvids.size() < this.parent.hits().total().value()) {
          page = this.client.scroll(new ScrollRequest.Builder()
              .scroll(new Time.Builder().time("24m").build()).scrollId(scrollID).build(), Object.class);
          scrollID = page.scrollId(); // docs say this can change
          for (Hit<Object> hit : page.hits().hits()) {
            lidvids.add(((Map<String,String>)hit.source()).get("lidvid"));
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException("How did we get here???", ioe);
      }
    }
    return lidvids;
  }
  @Override
  public List<String> latestLidvids() {
    ArrayList<String> lidvids = new ArrayList<String>();
    if (this.parent.aggregations() != null) {
      for (StringTermsBucket lidGroup : this.parent.aggregations().get("lidvids").sterms().buckets().array()) {
        String latest = "::-1.0";
        for (Hit<JsonData> hit : lidGroup.aggregations().get("groupbylid").topHits().hits().hits()) {
          int cmajor = Integer.parseInt(hit.id().split("::")[1].split("\\.")[0]);
          int cminor = Integer.parseInt(hit.id().split("::")[1].split("\\.")[1]);
          int lmajor = Integer.parseInt(latest.split("::")[1].split("\\.")[0]);
          int lminor = Integer.parseInt(latest.split("::")[1].split("\\.")[1]);
          if (cmajor > lmajor || (cmajor == lmajor && cminor > lminor)) {
            latest = hit.id();
          }
        }
        lidvids.add(latest);
      }
    }
    return lidvids;
  }
  @Override
  public LddVersions lddInfo() throws UnsupportedOperationException, IOException {
    LddVersions result = new LddVersions();
    for (Hit<Object> hit : this.parent.hits().hits()) {
      Map<String,String> source = (Map<String,String>)hit.source();
      if (source.containsKey("attr_name") && source.containsKey("date")) {
        result.addSchemaFile(source.get("attr_name"));
        result.updateDate(source.get("date"));
      } else {
        throw new UnsupportedOperationException("Either date or attr_name or both are missing from hit.");
      }
    }
    return result;
  }
  @Override
  public List<LddInfo> ldds() throws UnsupportedOperationException, IOException {
    ArrayList<LddInfo> results = new ArrayList<LddInfo>();
    if (parent.hits() != null) {
      for (Hit<Object> hit : parent.hits().hits()) {
        LddInfo item = new LddInfo();
        Map<String,String> src = (Map<String,String>)hit.source();
        item.namespace = src.get("attr_name");
        item.date = Instant.parse(src.get("date"));
        results.add(item);
      }
    }
    return results;
  }
  @Override
  public Set<String> nonExistingIds(Collection<String> from_ids)
      throws UnsupportedOperationException, IOException {
    HashSet<String> results = new HashSet<String>(from_ids);
    for (Hit<Object> hit : this.parent.hits().hits()) {
      if (from_ids.contains(hit.id())) results.remove(hit.id());
    }
    return results;
  }
  @Override
  public List<Object> batch() throws UnsupportedOperationException, IOException {
    return this.parent.documents();
  }
  @Override
  public String field(String name) throws NoSuchFieldException {
    return ((Map<String,Object>)this.parent.documents().get(0)).get(name).toString();
  }
  @Override
  public List<Map<String, Object>> documents() {
    ArrayList<Map<String,Object>> docs = new ArrayList<Map<String,Object>>();
    for (Object doc : this.parent.documents()) {
      docs.add((Map<String,Object>)doc);
    }
    if (docs.isEmpty()) { // try hits
      for (Hit<Object> hit : this.parent.hits().hits()) {
        docs.add((Map<String,Object>)hit.source());
      }
    }
    return docs;
  }
  @Override
  public List<String> bucketValues() {
    ArrayList<String> keys =  new ArrayList<String>();
    for (StringTermsBucket bucket : this.parent.aggregations().get("duplicates").sterms().buckets().array()) {
      keys.add(bucket.key());
    }
    return keys;
  }
}
