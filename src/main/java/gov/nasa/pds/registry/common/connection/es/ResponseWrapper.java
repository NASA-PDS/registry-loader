package gov.nasa.pds.registry.common.connection.es;

import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import gov.nasa.pds.registry.common.Response;

class ResponseWrapper implements Response {
  final org.elasticsearch.client.Response real_response;
  ResponseWrapper (org.elasticsearch.client.Response real_response) {
    this.real_response = real_response;
  }
  @Override
  public HttpEntity getEntity() {
    return this.real_response.getEntity();
  }
  @Override
  public StatusLine getStatusLine() {
    return this.real_response.getStatusLine();
  }
  @Override
  public void printWarnings() {
    List<String> warnings = this.real_response.getWarnings();
    if(warnings != null)
    {
      for(String warn: warnings)
      {
        System.out.println("[WARN] " + warn);
      }
    }
  }
}
