package gov.nasa.pds.registry.common;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.TreeMap;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import gov.nasa.pds.registry.common.util.CloseUtils;

public interface RestClient extends Closeable {
  public Request.Bulk createBulkRequest();
  public Request.Count createCountRequest();
  public Request.DeleteByQuery createDeleteByQuery();
  public Request.Get createGetRequest();
  public Request.Mapping createMappingRequest();
  public Request.MGet createMGetRequest();
  public Request.Search createSearchRequest();
  public Request.Setting createSettingRequest();
  public Response.CreatedIndex create (String indexName, String configAsJson) throws IOException,ResponseException;
  public void delete (String indexName) throws IOException,ResponseException;
  public boolean exists (String indexName) throws IOException,ResponseException;
  public Response.Bulk performRequest(Request.Bulk request) throws IOException,ResponseException;
  public long performRequest(Request.Count request) throws IOException,ResponseException;
  public long performRequest(Request.DeleteByQuery request) throws IOException,ResponseException;
  public Response.Get performRequest(Request.Get request) throws IOException,ResponseException;
  public Response.Mapping performRequest(Request.Mapping request) throws IOException,ResponseException;
  public Response.Search performRequest(Request.Search request) throws IOException,ResponseException;
  public Response.Settings performRequest(Request.Setting request) throws IOException,ResponseException;
  /**
   * Build create index request
   * 
   * @param schemaFile index schema file
   * @param shards number of shards
   * @param replicas number of replicas
   * @return JSON
   * @throws Exception Generic exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static String createCreateIndexRequest(File schemaFile, int shards, int replicas)
      throws Exception {
    // Read schema template
    FileReader rd = new FileReader(schemaFile);
    Gson gson = new Gson();
    Object rootObj = gson.fromJson(rd, Object.class);
    CloseUtils.close(rd);
    Object settingsObj = ((Map) rootObj).get("settings");
    if (settingsObj == null) {
      settingsObj = new TreeMap();
    }
    Object mappingsObj = ((Map) rootObj).get("mappings");
    if (mappingsObj == null) {
      throw new Exception("Missing mappings in schema file " + schemaFile.getAbsolutePath());
    }
    StringWriter out = new StringWriter();
    JsonWriter writer = new JsonWriter(out);
    writer.beginObject();
    Map settingsMap = (Map) settingsObj;
    settingsMap.put("number_of_shards", shards);
    settingsMap.put("number_of_replicas", replicas);
    // Settings
    writer.name("settings");
    gson.toJson(settingsObj, Object.class, writer);
    // Mappings
    writer.name("mappings");
    gson.toJson(mappingsObj, Object.class, writer);
    writer.endObject();
    writer.close();
    return out.toString();
  }
}
