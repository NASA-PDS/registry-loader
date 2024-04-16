package gov.nasa.pds.registry.common;

import java.io.Closeable;
import java.io.IOException;

public interface RestClient extends Closeable {
  public Request.Bulk createBulkRequest();
  public Request.Count createCountRequest();
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
  public Response.Get performRequest(Request.Get request) throws IOException,ResponseException;
  public Response.Mapping performRequest(Request.Mapping request) throws IOException,ResponseException;
  public Response.Search performRequest(Request.Search request) throws IOException,ResponseException;
  public Response.Settings performRequest(Request.Setting request) throws IOException,ResponseException;
}
