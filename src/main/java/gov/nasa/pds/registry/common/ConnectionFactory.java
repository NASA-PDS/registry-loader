package gov.nasa.pds.registry.common;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;

public interface ConnectionFactory {
  public ConnectionFactory clone();
  public RestClient createRestClient() throws Exception;
  public CredentialsProvider getCredentials();
  public org.apache.hc.client5.http.auth.CredentialsProvider getCredentials5();
  public HttpHost getHost();
  public org.apache.hc.core5.http.HttpHost getHost5();
  public String getHostName();
  public String getIndexName();
  public boolean isTrustingSelfSigned();
  public void reconnect() throws IOException, InterruptedException; // used when credentials time out otherwise not defined
  public ConnectionFactory setIndexName (String idxName);
}
