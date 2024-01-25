package gov.nasa.pds.registry.common;

import java.net.HttpURLConnection;

public interface ConnectionFactory {
  public HttpURLConnection createConnection();
  public String getHostName();
  public String getIndexName();
  public ConnectionFactory setAPI (String api);
  public ConnectionFactory setIndexName (String idxName);
}
