package gov.nasa.pds.registry.common.connection.aws;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.meta.Metadata;

class BulkImpl implements Bulk {
  final BulkRequest.Builder craftsman = new BulkRequest.Builder();
  final private boolean isServerless;
  BulkImpl (boolean isServerless) {
    this.isServerless = isServerless;
  }
  @SuppressWarnings("unchecked")
  @Override
  public void add(String statement, String document) {
    BulkOperation.Builder journeyman = new BulkOperation.Builder();
    Gson gson = new GsonBuilder()
        .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
        .create();
    Map<String,Map<String,String>> cmd;
    Map<String,String> params;
    Object doc = gson.fromJson(document, Object.class);
    Type contentType = new TypeToken<Map<String,Map<String,String>>>(){}.getType();
    cmd = gson.fromJson(statement, contentType);
    
    if (cmd.containsKey("create")) {
      CreateOperation.Builder<Object> apprentice = new CreateOperation.Builder<Object>().document(doc);
      params = cmd.get("create");
      if (params.containsKey("_id")) {
        apprentice.id(params.get("_id"));
      }
      if (params.containsKey("_index")) {
        apprentice.index(params.get("_index"));
      }
      journeyman.create(apprentice.build());
    } else if (cmd.containsKey("index")) {
      IndexOperation.Builder<Object> apprentice = new IndexOperation.Builder<Object>().document(doc);
      params = cmd.get("index");
      if (params.containsKey("_id")) {
        apprentice.id(params.get("_id"));
      }
      if (params.containsKey("_index")) {
        apprentice.index(params.get("_index"));
      }
      journeyman.index(apprentice.build());
    } else if (cmd.containsKey("update")) {
      doc = ((Map<String,Object>)doc).get("doc");
      UpdateOperation.Builder<Object> apprentice = new UpdateOperation.Builder<Object>().document(doc);
      params = cmd.get("update");
      if (params.containsKey("_id")) {
        apprentice.id(params.get("_id"));
      }
      if (params.containsKey("_index")) {
        apprentice.index(params.get("_index"));
      }
      journeyman.update(apprentice.build());      
    } else {
      throw new RuntimeException("Received a statement that did not contain one of the expected keys: create, index, or update.");
    }
    this.craftsman.operations(journeyman.build());
  }
  @Override
  public Bulk buildUpdateStatus(Collection<String> lidvids, String status) {
    ArrayList<BulkOperation> updates = new ArrayList<BulkOperation>();
    Map<String,List<String>> doc;
    for (String lidvid : lidvids) {
      doc = new HashMap<String,List<String>>();
      doc.put(Metadata.FLD_ARCHIVE_STATUS, Arrays.asList(status));
      updates.add(new BulkOperation.Builder().update(new UpdateOperation.Builder<Map<String,List<String>>>().document(doc).id(lidvid).build()).build());
    }
    this.craftsman.operations(updates);
    return this;
  }
  @Override
  public Bulk setIndex(String name) {
    this.craftsman.index(name);
    return this;
  }
  @Override
  public Bulk setRefresh(Refresh type) {
    switch (type) {
      case False:
        this.craftsman.refresh(org.opensearch.client.opensearch._types.Refresh.False);
        break;
      case True:
        this.craftsman.refresh(org.opensearch.client.opensearch._types.Refresh.True);
        break;
      case WaitFor:
        if (this.isServerless) {
          // No OP because not supported in serverless realm
        }
        else {
          this.craftsman.refresh(org.opensearch.client.opensearch._types.Refresh.WaitFor);
        }
        break;
    }
    return this;
  }
}
