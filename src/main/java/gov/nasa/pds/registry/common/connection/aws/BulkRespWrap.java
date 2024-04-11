package gov.nasa.pds.registry.common.connection.aws;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import gov.nasa.pds.registry.common.Response;

class BulkRespWrap implements Response.Bulk {
  private class ItemWrap implements Response.Bulk.Item {
    final BulkResponseItem parent;
    ItemWrap (BulkResponseItem parent) {
      this.parent = parent;
    }
    @Override
    public String id() {
      return this.parent.id();
    }
    @Override
    public String index() {
      return this.parent.index();
    }
    @Override
    public String result() {
      return this.parent.result();
    }
    @Override
    public int status() {
      return this.parent.status();
    }
    @Override
    public boolean error() {
      return this.parent.error() != null;
    }
    @Override
    public String operation() {
      return this.parent.operationType().jsonValue();
    }
    @Override
    public String reason() {
      return this.error() ? this.parent.error().reason() : "";
    }
  };
  final private ArrayList<Response.Bulk.Item> items = new ArrayList<Response.Bulk.Item>();
  final private BulkResponse parent;
  BulkRespWrap(BulkResponse parent) {
    this.parent = parent;
  }
  @Override
  public boolean errors() {
    return this.parent.errors();
  }
  @Override
  public synchronized List<Item> items() {
    if (this.parent.items().size() != this.items.size()) {
      for (BulkResponseItem item : this.parent.items()) {
        this.items.add(new ItemWrap(item));
      }
    }
    return this.items();
  }
  @Override
  public long took() {
    return this.parent.took();
  }
}
