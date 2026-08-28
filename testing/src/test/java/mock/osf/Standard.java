package mock.osf;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import mock.JsonHelper;
import mock.JsonHelper.BulkCreateRequest;
import mock.JsonHelper.BulkCreateResponse;
import mock.JsonHelper.BulkCreateResponseItem;
import mock.JsonHelper.BulkCreateResponseItemResult;
import mock.JsonHelper.BulkCreateShards;
import mock.JsonHelper.BulkIndexRequest;
import mock.JsonHelper.SearchHit;
import mock.JsonHelper.SearchHits;
import mock.JsonHelper.SearchShards;
import mock.JsonHelper.SearchTotal;
import mock.JsonHelper.SearchVersionsRequest;
import mock.JsonHelper.SearchVersionsResponse;
import mock.JsonHelper.SearchVersionsSource;
import mock.JsonHelper.SearchVersionsTool;
import mock.JsonHelper.SearchVersionsVersion;
import mock.OpensearchEngine;
import mock.annotation.Replace;

public class Standard extends NoOp {
  private final JsonHelper json = new JsonHelper();

  @Override @Replace
  public Response authorize (Context ctc) {
    if (ctc.headers().containsKey("authorization") && ctc.headers().get("authorization").startsWith("Basic ")) {
      return Response.empty(200);
    }
    return Response.empty(401);
  }
  @Override @Replace
  public Response postBulkCreate (Context ctx) {
    int seq = 0;
    Iterator<String> requestsText = List.of(ctx.body().split("\\R")).iterator();
    List<BulkCreateResponseItem> items = new LinkedList<>();
    while (requestsText.hasNext()) {
      BulkCreateRequest request = json.decode(requestsText.next(), BulkCreateRequest.class);
      items.add (new BulkCreateResponseItem(
          new BulkCreateResponseItemResult(
              request.create()._index(),
              request.create()._id(),
              1, "created",
              new BulkCreateShards(1, 1, 0),
              seq, 1, 201)));
      requestsText.next(); // throw away the body of the message
    }
    return Response.json(json.encode(new BulkCreateResponse(11, false, items)));
  }
  @Override @Replace
  public Response postBulkIndex (Context ctx) {
    int seq = 0;
    Iterator<String> requestsText = List.of(ctx.body().split("\\R")).iterator();
    List<BulkCreateResponseItem> items = new LinkedList<>();
    while (requestsText.hasNext()) {
      BulkIndexRequest request = json.decode(requestsText.next(), BulkIndexRequest.class);
      items.add (new BulkCreateResponseItem(
          new BulkCreateResponseItemResult(
              request.index()._index(),
              request.index()._id(),
              1, "created",
              new BulkCreateShards(1, 1, 0),
              seq, 1, 201)));
      requestsText.next(); // throw away the body of the message
    }
    return Response.json(json.encode(new BulkCreateResponse(11, false, items)));
  }
  @Override @Replace
  public Response postSearch(Context ctx) {
    List<String> keys = List.of("tool.name", "tool.version.major", "tool.version.minor", "tool.version.patch");
    if (keys.stream().allMatch(ctx.body()::contains)) {
      return OpensearchEngine.instance().process("postSearchVersions", ctx);
    }
    return super.postSearch(ctx);
  }
  @Override @Replace
  public Response postSearchVersions(Context ctx) {
    LinkedList<SearchHit> hits = new LinkedList<SearchHit>();
    SearchVersionsRequest  request = json.decode(ctx.body(), SearchVersionsRequest.class);
    String index = "dev-registry-structured-versions";
    for (String toolName : request.query().ids().values()) {
      hits.add(new SearchHit(
          index,
          toolName,
          1.0f,
          new SearchVersionsSource(new SearchVersionsTool(toolName, new SearchVersionsVersion(0,0,0)))));
    }
    return Response.json(json.encode(
        new SearchVersionsResponse(
            11,
            false,
            new SearchShards(hits.size(), hits.size(), 0, 0), 
            new SearchHits(
                new SearchTotal(hits.size(), "eq"),
                hits))));
  }
  @Override @Replace
  public Response putMappingsSettings (Context ctx) {
    return Response.json("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + ctx.index() + "\"}");
  }
}
