package gov.nasa.pds.registry.common;

import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.http.HttpHost;

public interface ConnectionFactory {
  public HttpURLConnection createConnection() throws IOException;
  public HttpHost getHost();
  public String getHostName();
  public String getIndexName();
  public ConnectionFactory setAPI (String api);
  public ConnectionFactory setIndexName (String idxName);
}
