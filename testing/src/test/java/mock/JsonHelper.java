package mock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHelper {
  public record BulkCreateRequest (BulkCreateRequestLine1 create) {};
  public record BulkCreateRequestLine1(String _id, String _index) {};
  public record BulkCreateResponse(int took, boolean errors, List<BulkCreateResponseItem> items) {};
  public record BulkCreateResponseItem(BulkCreateResponseItemResult create) {};
  public record BulkCreateResponseItemResult(String _index, String _id, int _version, String result, BulkCreateShards _shards, int _seq_no, int _primary_term, int status) {};
  public record BulkCreateShards(int total, int successful, int failed) {};
  public record SearchHit(String _index, String _id, float _score, SearchVersionsSource _source) {};
  public record SearchHits(SearchTotal total, List<SearchHit> hits) {};
  public record SearchTotal(int value, String relation) {};
  public record SearchVersionsRequest(SearchVersionsRequestSource _source, SearchVersionsRequestQuery query, int size) {};
  public record SearchVersionsRequestQuery(SearchVersionsRequestQueryIds ids) {};
  public record SearchVersionsRequestQueryIds(List<String> values) {};
  public record SearchVersionsRequestSource(List<String> includes) {};
  public record SearchVersionsResponse(int took, boolean timed_out, SearchShards _shards, SearchHits hits) {};
  public record SearchVersionsSource(SearchVersionsTool tool) {};
  public record SearchVersionsTool(String name, SearchVersionsVersion version) {};
  public record SearchVersionsVersion(int major, int minor, int patch) {};
  public record SearchShards(int total, int successful, int skipped, int failed) {};

  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private final ObjectMapper mapper = new ObjectMapper();

  public <T> T decode (String body, Class<T> target) {
    try {
      return this.mapper.readValue(body, target);
    } catch (JsonProcessingException e) {
      log.error("Could not convert body to the desired record {}", body, e);
      throw new NoOpException("invalid json body for conversion");
    }
  }
  
  public Map<String, String> decodeTopLevel(String body) {
    if (body == null || body.isBlank())
      return Map.of();
    try {
      JsonNode root = this.mapper.readTree(body);
      Map<String, String> result = new LinkedHashMap<>();
      if (!root.isObject())
        return Map.of(); // not a JSON object at top level
      for (Map.Entry<String, JsonNode> e : root.properties()) {
       result.put(e.getKey(), this.mapper.writeValueAsString(e.getValue()));
      }
      return result;
    } catch (Exception e) {
      return Map.of(); // not valid JSON at all
    }
  }
  
  public <T> String encode (T object) {
    try {
      return this.mapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      log.error("invalid object (not a record) for encoding to json", e);
      throw new NoOpException("invalid object for conversion to json");
    }
  }

}
