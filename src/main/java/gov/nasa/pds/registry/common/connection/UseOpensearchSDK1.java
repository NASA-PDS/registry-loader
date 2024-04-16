package gov.nasa.pds.registry.common.connection;

import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.connection.config.DirectType;
import gov.nasa.pds.registry.common.connection.es.RestClientWrapper;

public class UseOpensearchSDK1 implements ConnectionFactory {
  final private boolean veryTrusting;
  final private AuthContent auth;
  final private HttpHost host;
  final private org.apache.hc.core5.http.HttpHost host5;
  final private URL service;
  private String index = null;

  public static UseOpensearchSDK1 build (DirectType url, AuthContent auth) throws Exception {
    URL service = new URL(url.getValue());
    // Trust self-signed certificates
    if(url.isTrustSelfSigned())
    {
        SSLContext sslCtx = SSLUtils.createTrustAllContext();
        HttpsURLConnection.setDefaultSSLSocketFactory(sslCtx.getSocketFactory());
    }
    return new UseOpensearchSDK1 (service, auth, url.isTrustSelfSigned());
  }

  private UseOpensearchSDK1 (URL service, AuthContent auth, boolean trustSelfSigned) {
    this.auth = auth;
    this.host = new HttpHost(service.getHost(), service.getPort(), service.getProtocol());
    this.host5 = new org.apache.hc.core5.http.HttpHost(service.getProtocol(), service.getHost(), service.getPort());
    this.service = service;
    this.veryTrusting = trustSelfSigned;
  }
  @Override
  public ConnectionFactory clone() {
    return new UseOpensearchSDK1(this.service, this.auth, this.veryTrusting).setIndexName(this.index);
  }
  @Override
  public RestClient createRestClient() throws Exception {
    return new RestClientWrapper(this);
  }
  @Override
  public org.apache.hc.client5.http.auth.CredentialsProvider getCredentials5() {
   return this.auth.getCredentials5(this.getHost5()); 
  }
  @Override
  public HttpHost getHost() {
    return this.host;
  }
  @Override
  public String getHostName() {
    return this.host.getHostName();
  }
  @Override
  public org.apache.hc.core5.http.HttpHost getHost5() {
    return this.host5;
  }
  @Override
  public String getIndexName() {
    return this.index;
  }
  @Override
  public ConnectionFactory setIndexName(String idxName) {
    this.index =  idxName;
    return this;
  }
  @Override
  public String toString() {
    String me = "Direct to " + this.service.getProtocol() + "://" + this.service.getHost();
    if (0 <= this.service.getPort()) me += ":" + this.service.getPort();
    me += " using index '" + String.valueOf(this.index) + "'";
    return me;
  }
  @Override
  public CredentialsProvider getCredentials() {
    return this.auth.getCredentials();
  }
  @Override
  public boolean isTrustingSelfSigned() {
    return this.veryTrusting;
  }
}
