package gov.nasa.pds.registry.common.connection;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.connection.aws.RestClientWrapper;
import gov.nasa.pds.registry.common.connection.config.CognitoType;
import gov.nasa.pds.registry.common.connection.config.DirectType;

public final class UseOpensearchSDK2 implements ConnectionFactory {
  final private boolean isServerless;
  final private boolean veryTrusting;
  final private AuthContent auth;
  final private CognitoContent content;
  final private HttpHost host;
  final private org.apache.hc.core5.http.HttpHost host5;
  final private URL endpoint;
  private String index = null;
  public static UseOpensearchSDK2 build (CognitoType cog, AuthContent auth) throws IOException, InterruptedException {
    System.setProperty("org.opensearch.path.encoding","HTTP_CLIENT_V5_EQUIV"); // opensearch-java >= 2.13 not needed >= 3.x
    boolean expectedContent = true;
    Gson gson = new Gson();
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(cog.getIDP()))
        .POST(BodyPublishers.ofString("{\"AuthFlow\":\"USER_PASSWORD_AUTH\",\"AuthParameters\":{"
            + "\"USERNAME\":\"" + auth.getUser() + "\","
            + "\"PASSWORD\":\"" + auth.getPassword() + "\""
            + "},\"ClientId\":\"" + cog.getValue() + "\""
            + "}"))
        .setHeader("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")
        .setHeader("Content-Type", "application/x-amz-json-1.1")
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    Map<String,Map<String,String>> content;
    Type contentType = new TypeToken<Map<String,Map<String,String>>>(){}.getType();

    expectedContent &= response.body().contains("AuthenticationResult");
    expectedContent &= response.body().contains("AccessToken");
    expectedContent &= response.body().contains("ExpiresIn");
    expectedContent &= response.body().contains("IdToken");
    expectedContent &= response.body().contains("RefreshToken");
    expectedContent &= response.body().contains("TokenType");
    expectedContent &= response.body().contains("ChallengeParameters");
    if (!expectedContent) {
      throw new IOException("Received an unexpected response of: " + response.toString()
      + " ->\n" + response.body());
    }
    content = gson.fromJson(response.body(), contentType);
    return new UseOpensearchSDK2(auth, new URL(cog.getEndpoint()), true, false)
        .update(content, cog.getValue(), cog.getIDP(), cog.getGateway())
        .tokensToKeys();
  }
  private UseOpensearchSDK2 update (Map<String,Map<String,String>> content, String cid, String idp, String gateway) {
    this.content.accessToken = content.get("AuthenticationResult").get("AccessToken");
    this.content.clientid = cid;
    this.content.gateway = gateway;
    this.content.idp = idp;
    this.content.idToken = content.get("AuthenticationResult").get("IdToken");
    this.content.refreshToken = content.get("AuthenticationResult").get("RefreshToken");
    this.content.tokenType = content.get("AuthenticationResult").get("TokenType");
    return this;
  }
  private UseOpensearchSDK2 update (CognitoContent old) {
    if (this.isServerless) {
      this.content.accessToken = old.accessToken;
      this.content.clientid = old.clientid;
      this.content.gateway = old.gateway;
      this.content.idp  = old.idp;
      this.content.idToken = old.idToken;
      this.content.refreshToken = old.refreshToken;
      this.content.tokenType = old.tokenType;
    }
    return this;
  }
  private UseOpensearchSDK2 tokensToKeys() throws IOException, InterruptedException {
    boolean expectedContent = true;
    Gson gson = new Gson();
    HttpClient client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(this.content.gateway))
        .GET()
        .setHeader("Authorization", this.content.tokenType + " " + this.content.accessToken)
        .setHeader("IDToken", this.content.idToken)
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    Map<String,Map<String,String>> content;
    Properties awsCreds = new Properties(System.getProperties()); // initialize properties as oracle suggests
    Type contentType = new TypeToken<Map<String,Map<String,String>>>(){}.getType();
    if (299 < response.statusCode()) {
      throw new IOException("Could not obtain credentials: " + response.toString());
    }
    expectedContent &= response.body().contains("body");
    expectedContent &= response.body().contains("AccessKeyId");
    expectedContent &= response.body().contains("SecretAccessKey");
    expectedContent &= response.body().contains("SessionToken");
    if (!expectedContent) {
      throw new IOException("Did not find expected credential response from: " + response.toString());
    }
    content = gson.fromJson(response.body(), new TypeToken<Map<String,Object>>(){}.getType());
    content = gson.fromJson("{\"Credentials\":" + content.get("body") + "}", contentType);    
    // fill then set system properties as oracle suggests (init happened above)
    awsCreds.setProperty("aws.accessKeyId", content.get("Credentials").get("AccessKeyId"));
    awsCreds.setProperty("aws.secretAccessKey", content.get("Credentials").get("SecretAccessKey"));
    awsCreds.setProperty("aws.sessionToken", content.get("Credentials").get("SessionToken"));
    System.setProperties(awsCreds);
    return this;
  }
  public static UseOpensearchSDK2 build (DirectType url, AuthContent auth) throws Exception {
    return new UseOpensearchSDK2(auth, new URL(url.getValue()), false, url.isTrustSelfSigned());
  }
  private UseOpensearchSDK2 (AuthContent auth, URL opensearchEndpoint, boolean isServerless, boolean veryTrusting) {
    this.auth = auth;
    this.content = new CognitoContent();
    this.endpoint = opensearchEndpoint;
    this.host = new HttpHost(this.endpoint.getHost(), this.endpoint.getPort(), this.endpoint.getProtocol());
    this.host5 = new org.apache.hc.core5.http.HttpHost(this.endpoint.getProtocol(), this.endpoint.getHost(), this.endpoint.getPort());
    this.isServerless = isServerless;
    this.veryTrusting = veryTrusting;
  }
  @Override
  public ConnectionFactory clone() {
    return new UseOpensearchSDK2(this.auth, this.endpoint, this.isServerless, this.veryTrusting)
        .update(this.content)
        .setIndexName(this.index);
  }
  @Override
  public RestClient createRestClient() throws Exception {
    return new RestClientWrapper(this, this.isServerless);
  }
  @Override
  public CredentialsProvider getCredentials() {
    return this.auth.getCredentials(this.getHost());
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
  public org.apache.hc.core5.http.HttpHost getHost5() {
    return this.host5;
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
  public boolean isTrustingSelfSigned() {
    return this.veryTrusting;
  }
  @Override
  public ConnectionFactory setIndexName(String idxName) {
    this.index = idxName;
    return this;
  }
  @Override
  public void reconnect() throws IOException, InterruptedException {
    if (this.isServerless) {
      boolean expectedContent = true;
      Gson gson = new Gson();
      String cid = "47d9j6ks9un4errq6pnbu0bc1r";
      HttpClient client = HttpClient.newHttpClient();
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(this.content.idp))
          .POST(BodyPublishers.ofString("{\"AuthFlow\":\"REFRESH_TOKEN_AUTH\",\"AuthParameters\":{"
              + "\"REFRESH_TOKEN\":\"" + this.content.refreshToken + "\""
              + "},\"ClientId\":\"" + cid + "\""
              + "}"))
          .setHeader("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")
          .setHeader("Content-Type", "application/x-amz-json-1.1")
          .build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      Map<String,Map<String,String>> content;
      Type contentType = new TypeToken<Map<String,Map<String,String>>>(){}.getType();

      expectedContent &= response.body().contains("AuthenticationResult");
      expectedContent &= response.body().contains("AccessToken");
      expectedContent &= response.body().contains("ExpiresIn");
      expectedContent &= response.body().contains("IdToken");
      expectedContent &= response.body().contains("TokenType");
      expectedContent &= response.body().contains("ChallengeParameters");
      if (!expectedContent) {
        throw new IOException("Received an unexpected response of: " + response.toString()
        + " ->\n" + response.body());
      }
      content = gson.fromJson(response.body(), contentType);
      content.get("AuthenticationResult").put("RefreshToken", this.content.refreshToken);
      this.update(content, this.content.clientid, this.content.idp, this.content.gateway);
      this.tokensToKeys();
    }
  }
}
