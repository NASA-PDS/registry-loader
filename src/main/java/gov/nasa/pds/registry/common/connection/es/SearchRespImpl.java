package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.es.dao.LatestLidsResponseParser;
import gov.nasa.pds.registry.common.es.dao.dd.LddInfo;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;

class SearchRespImpl implements Response.Search {
  private static class BatchObjects implements SearchResponseParser.Callback {
    final ArrayList<Object> content = new ArrayList<Object>();
    public void onRecord(String id, Object rec) {
      content.add(rec);
    }
  }
  private static class FieldNameFinder implements SearchResponseParser.Callback {
    final private String fieldName;
    boolean found = false;
    String blob = null;
    @Override
    @SuppressWarnings("rawtypes")
    public void onRecord(String id, Object rec) {
      found = true;
      this.blob = ((Map) rec).get(this.fieldName).toString();
    }
    public FieldNameFinder(String fieldName) {
      super();
      this.fieldName = fieldName;
    }
  }

  /**
   * Inner private class to parse LDD information response from Elasticsearch.
   * 
   * @author karpenko
   */
  private static class ListFieldsParser extends SearchResponseParser
      implements SearchResponseParser.Callback {
    public Set<String> list;
    public ListFieldsParser() {
      list = new HashSet<>(200);
    }
    @Override
    public void onRecord(String id, Object rec) {
      if (rec instanceof Map) {
        @SuppressWarnings("rawtypes")
        Map map = (Map) rec;
        String fieldName = (String) map.get("es_field_name");
        list.add(fieldName);
      }
    }
  }
  /**
   * Inner private class to parse LDD information response from Elasticsearch.
   * 
   * @author karpenko
   */
  private static class GetLddInfoRespParser extends SearchResponseParser
      implements SearchResponseParser.Callback {
    public LddVersions info;
    public GetLddInfoRespParser() {
      info = new LddVersions();
    }
    @Override
    public void onRecord(String id, Object rec) {
      if (rec instanceof Map) {
        @SuppressWarnings("rawtypes")
        Map map = (Map) rec;
        String strDate = (String) map.get("date");
        info.updateDate(strDate);
        String file = (String) map.get("attr_name");
        info.addSchemaFile(file);
      }
    }
  }
  /**
   * Inner private class to parse LDD information response from Elasticsearch.
   * 
   * @author karpenko
   */
  private static class ListLddsParser extends SearchResponseParser
      implements SearchResponseParser.Callback {
    public List<LddInfo> list;
    public ListLddsParser() {
      list = new ArrayList<>();
    }
    @Override
    public void onRecord(String id, Object rec) {
      if (rec instanceof Map) {
        @SuppressWarnings("rawtypes")
        Map map = (Map) rec;
        LddInfo info = new LddInfo();
        // Namespace
        info.namespace = (String) map.get("attr_ns");
        // Date
        String str = (String) map.get("date");
        if (str != null && !str.isEmpty()) {
          info.date = Instant.parse(str);
        }
        // Versions
        info.imVersion = (String) map.get("im_version");
        // File name
        info.file = (String) map.get("attr_name");
        list.add(info);
      }
    }
  }

  final private org.elasticsearch.client.Response response;
  SearchRespImpl(org.elasticsearch.client.Response response) {
    this.response = response;
  }
  @Override
  public List<String> latestLidvids() {
    try (InputStream is = this.response.getEntity().getContent()) {
      LatestLidsResponseParser parser = new LatestLidsResponseParser();
      parser.parse(new InputStreamReader(is));
      return parser.getLidvids();
    } catch (UnsupportedOperationException | IOException e) {
      throw new RuntimeException("Weird JSON parsing error and should never get here");
    }
  }
  @Override
  public LddVersions lddInfo() throws UnsupportedOperationException, IOException {
    GetLddInfoRespParser parser = new GetLddInfoRespParser();
    parser.parseResponse(this.response, parser);
    return parser.info;
  }
  @Override
  public List<LddInfo> ldds() throws UnsupportedOperationException, IOException {
    ListLddsParser parser = new ListLddsParser();
    parser.parseResponse(this.response, parser); 
    return parser.list;
  }
  @Override
  public Set<String> fields() throws UnsupportedOperationException, IOException {
    ListFieldsParser parser = new ListFieldsParser();
    parser.parseResponse(this.response, parser); 
    return parser.list;
  }
  @Override
  public Map<String, Set<String>> altIds() throws UnsupportedOperationException, IOException {
    GetAltIdsParser cb = new GetAltIdsParser();
    SearchResponseParser parser = new SearchResponseParser();
    parser.parseResponse(this.response, cb);
    return cb.getIdMap();
  }
  @Override
  public Set<String> nonExistingIds(Collection<String> from_ids) throws UnsupportedOperationException, IOException {
    NonExistingIdsResponse idsResp = new NonExistingIdsResponse(from_ids);
    SearchResponseParser parser = new SearchResponseParser();
    parser.parseResponse(this.response, idsResp);
    return idsResp.getIds();
  }
  @Override
  public String field(String name) throws NoSuchFieldException {
    FieldNameFinder fnf = new FieldNameFinder(name);
    if (!fnf.found) throw new NoSuchFieldException();
    return fnf.blob;
  }
  @Override
  public List<Object> batch() throws UnsupportedOperationException, IOException {
    BatchObjects content = new BatchObjects();
    SearchResponseParser parser = new SearchResponseParser();
    parser.parseResponse(this.response, content);
    return content.content;
  }
  @Override
  public List<Map<String, Object>> documents() {
    throw new NotImplementedException();
  }
  @Override
  public List<String> lidvids() {
    throw new NotImplementedException();
  }
}
