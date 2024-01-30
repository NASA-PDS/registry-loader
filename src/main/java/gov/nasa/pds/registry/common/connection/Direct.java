package gov.nasa.pds.registry.common.connection;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.connection.es.RestClientWrapper;

public class Direct implements Cloneable, ConnectionFactory {
  final private boolean veryTrusting;
  final private int timeout = 5000;
  final private AuthContent auth;
  final private HttpHost host;
  final private URL service;
  private String api = null;
  private String index = null;

  public static Direct build (String url, AuthContent auth, boolean trustSelfSigned) throws Exception {
    URL service = new URL(url);
    // Trust self-signed certificates
    if(trustSelfSigned)
    {
        SSLContext sslCtx = SSLUtils.createTrustAllContext();
        HttpsURLConnection.setDefaultSSLSocketFactory(sslCtx.getSocketFactory());
    }
    return new Direct (service, auth, trustSelfSigned);
  }

  private Direct (URL service, AuthContent auth, boolean trustSelfSigned) {
    this.auth = auth;
    this.host = new HttpHost(service.getHost(), service.getPort(), service.getProtocol());
    this.service = service;
    this.veryTrusting = trustSelfSigned;
  }
  @Override
  public ConnectionFactory clone() {
    return new Direct(this.service, this.auth, this.veryTrusting).setAPI(this.api).setIndexName(this.index);
  }
  @Override
  public HttpURLConnection createConnection() throws IOException {
    String url = this.service.toString();
    if (this.index != null) url += "/" + this.index;
    if (this.api != null) url += "/" + this.api;
    HttpURLConnection con = (HttpURLConnection)new URL(url).openConnection();
    con.setConnectTimeout(this.timeout);
    con.setReadTimeout(this.timeout);
    con.setAllowUserInteraction(false);
    con.setRequestProperty("Authorization", this.auth.getHeader());
    return con;
  }
  @Override
  public RestClient createRestClient() throws Exception {
    return new RestClientWrapper(this);
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
  public String getIndexName() {
    return this.index;
  }
  @Override
  public ConnectionFactory setAPI(String api) {
    this.api = api;
    return this;
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
    me += " to call API '" + String.valueOf(this.api) + "'";
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
