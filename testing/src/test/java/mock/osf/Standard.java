package mock.osf;

import mock.annotation.Replace;

public class Standard extends NoOp {
  @Override @Replace
  public Response authorize (Context ctc) {
    if (ctc.headers().containsKey("authorization") && ctc.headers().get("authorization").startsWith("Basic ")) {
      return Response.empty(200);
    }
    return Response.empty(401);
  }
  @Override @Replace
  public Response putMappingsSettings (Context ctx) {
    return Response.json("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"dev-registry-structured\"}");
  }
}
