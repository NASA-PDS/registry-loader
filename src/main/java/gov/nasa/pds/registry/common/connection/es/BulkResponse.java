package gov.nasa.pds.registry.common.connection.es;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import gov.nasa.pds.registry.common.Response;

class BulkResponse implements Response {
  final String lastLine;
  BulkResponse(String lastLine) {
    this.lastLine = lastLine;
  }
  @Override
  public HttpEntity getEntity() {
    return null;
  }
  @Override
  public StatusLine getStatusLine() {
    return null;
  }
  @Override
  public void printWarnings() {
  }
  @Override
  public String toString() {
    return this.lastLine;
  }
}
