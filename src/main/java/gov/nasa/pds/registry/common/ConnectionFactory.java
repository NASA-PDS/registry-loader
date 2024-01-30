package gov.nasa.pds.registry.common;

import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;

public interface ConnectionFactory {
  public HttpURLConnection createConnection() throws IOException;
  public RestClient createRestClient();
  public CredentialsProvider getCredentials();
  public HttpHost getHost();
  public String getHostName();
  public String getIndexName();
  public boolean isTrustingSelfSigned();
  public ConnectionFactory setAPI (String api);
  public ConnectionFactory setIndexName (String idxName);
}
