package mock.osf;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import mock.JsonHelper;
import mock.JsonHelper.BulkCreateRequest;
import mock.JsonHelper.BulkCreateResponse;
import mock.JsonHelper.BulkCreateResponseItem;
import mock.JsonHelper.BulkCreateResponseItemResult;
import mock.JsonHelper.Shards;
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
              new Shards(1, 1, 0),
              seq, 1, 201)));
      requestsText.next(); // throw away the body of the message
    }
    return Response.json(json.encode(new BulkCreateResponse(11, false, items)));
  }
  @Override @Replace
  public Response putMappingsSettings (Context ctx) {
    return Response.json("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + ctx.index() + "\"}");
  }
}
