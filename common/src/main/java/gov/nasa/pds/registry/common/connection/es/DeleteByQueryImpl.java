package gov.nasa.pds.registry.common.connection.es;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import com.google.gson.Gson;
import gov.nasa.pds.registry.common.Request.DeleteByQuery;

class DeleteByQueryImpl implements DeleteByQuery {
  private boolean refresh = false;
  private String index = "undefnned";
  String query = "";
  @Override
  public DeleteByQuery setIndex(String name) {
    this.index = name;
    return this;
  }
  @Override
  public DeleteByQuery createFilterQuery(String key, String value) {
    this.query = new RegistryRequestBuilder().createFilterQuery(key, value);
    return this;
  }
  @Override
  public DeleteByQuery createMatchAllQuery() {
    this.query = new RegistryRequestBuilder().createMatchAllQuery();
    return this;
  }
  @Override
  public String toString() {
    return "/" + this.index + "/_delete_by_query" + (this.refresh ? "?refresh=true" : "");
  }
  @SuppressWarnings("rawtypes")
  long extractNumDeleted(org.elasticsearch.client.Response resp) {
    try {
      InputStream is = resp.getEntity().getContent();
      Reader rd = new InputStreamReader(is);
      Gson gson = new Gson();
      Object obj = gson.fromJson(rd, Object.class);
      rd.close();
      obj = ((Map) obj).get("deleted");
      return ((Double)obj).longValue();
    } catch (Exception ex) {
      return 0;
    }
  }
  @Override
  public DeleteByQuery setRefresh(boolean state) {
    this.refresh = state;
    return this;
  }
}
