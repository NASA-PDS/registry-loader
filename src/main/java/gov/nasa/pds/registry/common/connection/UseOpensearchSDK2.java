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
  final private HttpHost host;
  final private org.apache.hc.core5.http.HttpHost host5;
  final private URL endpoint;
  private String index = null;
  public static UseOpensearchSDK2 build (CognitoType cog, AuthContent auth) throws IOException, InterruptedException {
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
    Properties awsCreds = new Properties(System.getProperties()); // initialize properties as oracle suggests
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
    client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    request = HttpRequest.newBuilder()
        .uri(URI.create(cog.getGateway()))
        .GET()
        .setHeader("Authorization", content.get("AuthenticationResult").get("TokenType") + " " + content.get("AuthenticationResult").get("AccessToken"))
        .setHeader("IDToken", content.get("AuthenticationResult").get("IdToken"))
        .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (299 < response.statusCode()) {
      throw new IOException("Could not obtain signed URL: " + response.toString());
    }
    expectedContent &= response.body().contains("IdentityId");
    expectedContent &= response.body().contains("Credentials");
    expectedContent &= response.body().contains("AccessKeyId");
    expectedContent &= response.body().contains("SecretKey");
    expectedContent &= response.body().contains("SessionToken");
    expectedContent &= response.body().contains("Expiration");
    expectedContent &= response.body().contains("ResponseMetadata");
    contentType = new TypeToken<Map<String,Object>>(){}.getType();
    content = gson.fromJson(response.body(), contentType);
    // fill then set system properties as oracle suggests (init happened above)
    awsCreds.setProperty("aws.accessKeyId", content.get("Credentials").get("AccessKeyId"));
    awsCreds.setProperty("aws.secretAccessKey", content.get("Credentials").get("SecretKey"));
    awsCreds.setProperty("aws.sessionToken", content.get("Credentials").get("SessionToken"));
    System.setProperties(awsCreds);
    return new UseOpensearchSDK2(auth, new URL(cog.getEndpoint()), true, false);
  }
  public static UseOpensearchSDK2 build (DirectType url, AuthContent auth) throws Exception {
    return new UseOpensearchSDK2(auth, new URL(url.getValue()), false, url.isTrustSelfSigned());
  }
  private UseOpensearchSDK2 (AuthContent auth, URL opensearchEndpoint, boolean isServerless, boolean veryTrusting) {
    this.auth = auth;
    this.endpoint = opensearchEndpoint;
    this.host = new HttpHost(this.endpoint.getHost(), this.endpoint.getPort(), this.endpoint.getProtocol());
    this.host5 = new org.apache.hc.core5.http.HttpHost(this.endpoint.getProtocol(), this.endpoint.getHost(), this.endpoint.getPort());
    this.isServerless = isServerless;
    this.veryTrusting = veryTrusting;
  }
  @Override
  public ConnectionFactory clone() {
    return new UseOpensearchSDK2(this.auth, this.endpoint, this.isServerless, this.veryTrusting).setIndexName(this.index);
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
}
