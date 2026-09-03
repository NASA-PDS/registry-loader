package mock;

import java.util.Map;

public interface OpensearchSupportedFunctionality {
  public interface BulkDataHandler {
    public void document (String id, String bulkLine);
  }
  public record Context(
      String index, // the index that the JSON body applies
      String body, // original headers sent to the socket
      Map<String, String> headers, // original headers sent to the socket
      Map<String, String> queryParams, // original query parameters sent to the socket
      Map<String, String> pathParams) { // original path parameters sent to the socket
  }

  public record Response(
      int statusCode,
      String body,
      String contentType) {
      // Convenience factory for standard 200 OK JSON responses
      public static Response json(String jsonBody) {
          return new Response(200, jsonBody, "application/json");
      }      
      // Convenience factory for standard empty success responses
      public static Response empty(int statusCode) {
          return new Response(statusCode, "{}", "application/json");
      }
  }
  
  public Response authorize(Context ctx);
  public Response getMapping(Context ctx);
  public Response head(Context ctx);
  public Response postBulkCreate(Context ctx);
  public Response postBulkIndex(Context ctx);
  public Response postMgetIds(Context ctx);
  public Response postSearch(Context ctx);
  public Response postSearch_sourceQuerySize(Context ctx);
  public Response postSearchVersions(Context ctx);
  public Response putMappingProperties(Context ctx);
  public Response putMappingsSettings(Context ctx);
}
