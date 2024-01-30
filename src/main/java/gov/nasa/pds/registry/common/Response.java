package gov.nasa.pds.registry.common;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;

public interface Response {
  public HttpEntity getEntity();
  public StatusLine getStatusLine();
}
