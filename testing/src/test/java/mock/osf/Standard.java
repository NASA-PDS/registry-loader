package mock.osf;

import java.util.HashMap;
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
import mock.JsonHelper.DDEntry;
import mock.JsonHelper.MgetIds;
import mock.JsonHelper.MgetIdsResponse;
import mock.JsonHelper.MgetIdsResponseDoc;
import mock.JsonHelper.MgetIdsResponseDocSource;
import mock.JsonHelper.SearchHit;
import mock.JsonHelper.SearchHits;
import mock.JsonHelper.SearchShards;
import mock.JsonHelper.SearchTotal;
import mock.JsonHelper.SearchVersionsRequest;
import mock.JsonHelper.SearchVersionsResponse;
import mock.JsonHelper.SearchVersionsSource;
import mock.JsonHelper.SearchVersionsTool;
import mock.JsonHelper.SearchVersionsVersion;
import mock.OpensearchSupportedFunctionality.Context;
import mock.OpensearchSupportedFunctionality.Response;
import mock.OpensearchEngine;
import mock.annotation.Replace;

public class Standard extends NoOp {
  private final BulkDataHandler ignoreBulkData = new BulkDataHandler() { public void document(String id, String bulkLine) {} };
  private final JsonHelper json = new JsonHelper();
  private final HashMap<String,DDEntry> dd = new HashMap<String,DDEntry>();

  @Override @Replace
  public Response authorize (Context ctc) {
    if (ctc.headers().containsKey("authorization") && ctc.headers().get("authorization").startsWith("Basic ")) {
      return Response.empty(200);
    }
    return Response.empty(401);
  }
  @Override @Replace
  public Response getMapping(Context ctx) {
    if ("dev-registry-structured".equals(ctx.index())) {
      return Response.json("{\"dev-registry-structured\":{\"mappings\":{\"dynamic\":\"false\",\"dynamic_templates\":[{\"strings\":{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"keyword\"}}}],\"properties\":{\"_package_id\":{\"type\":\"keyword\"},\"alternate_ids\":{\"type\":\"keyword\"},\"description\":{\"type\":\"text\",\"analyzer\":\"english\"},\"lid\":{\"type\":\"keyword\"},\"lidvid\":{\"type\":\"keyword\"},\"ops:Data_File_Info\":{\"properties\":{\"ops:creation_date_time\":{\"type\":\"date\"},\"ops:file_name\":{\"type\":\"keyword\"},\"ops:file_ref\":{\"type\":\"keyword\"},\"ops:file_size\":{\"type\":\"long\"},\"ops:md5_checksum\":{\"type\":\"keyword\"},\"ops:mime_type\":{\"type\":\"keyword\"}}},\"ops:Harvest_Info\":{\"properties\":{\"ops:harvest_date_time\":{\"type\":\"date\"},\"ops:harvest_version\":{\"type\":\"keyword\"},\"ops:node_name\":{\"type\":\"keyword\"}}},\"ops:Label_File_Info\":{\"properties\":{\"ops:blob\":{\"type\":\"binary\"},\"ops:creation_date_time\":{\"type\":\"date\"},\"ops:file_name\":{\"type\":\"keyword\"},\"ops:file_ref\":{\"type\":\"keyword\"},\"ops:file_size\":{\"type\":\"long\"},\"ops:json_blob\":{\"type\":\"binary\"},\"ops:md5_checksum\":{\"type\":\"keyword\"}}},\"ops:Provenance\":{\"properties\":{\"ops:superseded_by\":{\"type\":\"keyword\"}}},\"ops:Tracking_Meta\":{\"properties\":{\"ops:archive_status\":{\"type\":\"keyword\"}}},\"product_class\":{\"type\":\"keyword\"},\"ref_lid_collection\":{\"type\":\"keyword\"},\"ref_lid_collection_secondary\":{\"type\":\"keyword\"},\"ref_lid_document\":{\"type\":\"keyword\"},\"ref_lid_instrument\":{\"type\":\"keyword\"},\"ref_lid_instrument_host\":{\"type\":\"keyword\"},\"ref_lid_investigation\":{\"type\":\"keyword\"},\"ref_lid_target\":{\"type\":\"keyword\"},\"title\":{\"type\":\"text\",\"analyzer\":\"english\"},\"vid\":{\"type\":\"float\"}}}}}");
    } else {
      return super.getMapping(ctx);
    }
  }
  @Override @Replace
  public Response head(Context ctx) {
    return Response.empty(200);
    }
  @Override @Replace
  public Response postBulkCreate (Context ctx) {
    int seq = 0;
    BulkDataHandler handle = this.ignoreBulkData;
    Iterator<String> requestsText = List.of(ctx.body().split("\\R")).iterator();
    List<BulkCreateResponseItem> items = new LinkedList<>();
    while (requestsText.hasNext()) {
      BulkCreateRequest request = json.decode(requestsText.next(), BulkCreateRequest.class);
      items.add (new BulkCreateResponseItem(
          new BulkCreateResponseItemResult(
              ctx.index() == null || ctx.index().isBlank() ? request.create()._index() : ctx.index(),
              request.create()._id(),
              1, "created",
              new BulkCreateShards(1, 1, 0),
              seq, 1, 201)));
      handle.document(request.create()._id(), requestsText.next());
    }
    return Response.json(json.encode(new BulkCreateResponse(11, false, items)));
  }
  @Override @Replace
  public Response postBulkIndex (Context ctx) {
    BulkDataHandler handler = this.ignoreBulkData;
    if ("dev-registry-structured-dd".equals(ctx.index())) {
      handler = new BulkDataHandler() { public void document(String id, String bulkLine) {
        dd.put(id, json.decode(bulkLine, DDEntry.class));
        }};
    }
    return this.postBulkIndex(ctx, handler);
  }
  private Response postBulkIndex (Context ctx, BulkDataHandler handle) {
    int seq = 0;
    Iterator<String> requestsText = List.of(ctx.body().split("\\R")).iterator();
    List<BulkCreateResponseItem> items = new LinkedList<>();
    while (requestsText.hasNext()) {
      BulkIndexRequest request = json.decode(requestsText.next(), BulkIndexRequest.class);
      items.add (new BulkCreateResponseItem(
          new BulkCreateResponseItemResult(
              ctx.index() == null || ctx.index().isBlank() ? request.index()._index() : ctx.index(),
              request.index()._id(),
              1, "created",
              new BulkCreateShards(1, 1, 0),
              seq, 1, 201)));
      handle.document(request.index()._id(), requestsText.next());
    }
    return Response.json(json.encode(new BulkCreateResponse(11, false, items)));
  }
  int i = 1;
  @Override @Replace
  public Response postMgetIds(Context ctx) {
    if ("dev-registry-structured-dd".equals(ctx.index())) {
      int seq = 13;
      LinkedList<MgetIdsResponseDoc> docs = new LinkedList<MgetIdsResponseDoc>();
      for (String id : json.decode(ctx.body(), MgetIds.class).ids()) {
        if (this.dd.containsKey(id)) {
          docs.add(new MgetIdsResponseDoc(id, ctx.index(), 1, seq++, 1, true, new MgetIdsResponseDocSource(this.dd.get(id).es_data_type())));
        } else {
          docs.add(new MgetIdsResponseDoc(id, ctx.index(), null, null, null, false, null));
        }
      }
      return Response.json(json.encode(new MgetIdsResponse(docs)));
    }
    return super.postMgetIds(ctx);
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
  public Response postSearch_sourceQuerySize(Context ctx) {
    if ("dev-registry-structured".equals(ctx.index())) {
       return Response.json("{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"max_score\":null,\"hits\":[]}}");
    }
    if ("dev-registry-structured-dd".equals(ctx.index())) {
      if (this.dd.size() == 0) {
        return Response.json("{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":{\"value\":0,\"relation\":\"eq\"},\"max_score\":null,\"hits\":[]}}");
      }
      if (ctx.body().contains("{\"includes\":[\"date\",\"attr_name\"]},\"query\":{\"bool\":{\"must\":[{\"match\":{\"class_ns\":{\"query\":\"registry\"}}},{\"match\":{\"class_name\":{\"query\":\"LDD_Info\"}}},{\"match\":{\"attr_ns\":{\"query\":\"pds\"}}}]}}")) {
        return Response.json("{\"took\":21,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},\"hits\":{\"total\":{\"value\":1,\"relation\":\"eq\"},\"max_score\":13.27233,\"hits\":[{\"_index\":\"dev-registry-structured-dd\",\"_id\":\"registry:LDD_Info.pds:PDS4_PDS_1500.JSON\",\"_score\":13.27233,\"_source\":{\"date\":\"2015-09-26T08:38:56Z\",\"attr_name\":\"PDS4_PDS_1500.JSON\"}}]}}");
      }
      if (ctx.body().contains("{\"includes\":[\"es_field_name\"]},\"query\":{\"bool\":{\"must\":[{\"match\":{\"es_data_type\":{\"query\":\"boolean\"}}}]}}")) {
        for (DDEntry entry : dd.values()) {
          if ("boolean".equals(entry.es_data_type())) {
            
          }
        }
        return ;
      }
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
  public Response putMappingProperties(Context ctx) {
    if ("dev-registry-structured".equals(ctx.index())) {
      return Response.json("{\"acknowledged\":true}");
   }
    return super.putMappingProperties(ctx);
  }
  @Override @Replace
  public Response putMappingsSettings (Context ctx) {
    return Response.json("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + ctx.index() + "\"}");
  }
}
