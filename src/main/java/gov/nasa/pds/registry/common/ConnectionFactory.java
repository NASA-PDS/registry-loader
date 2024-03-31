package gov.nasa.pds.registry.common;

import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;

public interface ConnectionFactory {
  public RestClient createRestClient() throws Exception;
  public CredentialsProvider getCredentials();
  public HttpHost getHost();
  public String getHostName();
  public String getIndexName();
  public boolean isTrustingSelfSigned();
  public ConnectionFactory setIndexName (String idxName);
}
