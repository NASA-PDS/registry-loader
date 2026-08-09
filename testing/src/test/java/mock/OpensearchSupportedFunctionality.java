package mock;

import java.util.Map;

public interface OpensearchSupportedFunctionality {
  public record Context(
      String body,
      Map<String, String> headers,
      Map<String, String> queryParams,
      Map<String, String> pathParams) {
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
  public Response getRoot(Context ctx);
  public Response postBulk(Context ctx);
}
