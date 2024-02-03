package gov.nasa.pds.registry.common.connection;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import gov.nasa.pds.registry.common.CognitoContent;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;

public final class MultiTenancy implements ConnectionFactory {
  final private AuthContent auth;
  final private HttpHost host;
  final private URL signed;
  private String api = null;
  private String index = null;
  public static MultiTenancy build (CognitoContent cog, AuthContent auth) throws IOException, InterruptedException {
    boolean expectedContent = true;
    Gson gson = new Gson();
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(cog.getIDP()))
        .POST(BodyPublishers.ofString("{\"AuthFlow\":\"USER_PASSWORD_AUTH\",\"AuthParameters\":{"
            + "\"USERNAME\":\"" + auth.getUser() + "\","
            + "\"PASSWORD\":\"" + auth.getPassword() + "\""
            + "},\"ClientId\":\"" + cog.getClientID() + "\""
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
    client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    request = HttpRequest.newBuilder()
        .uri(URI.create(cog.getGateway()))
        .GET()
        .setHeader("Authorization", content.get("AuthenticationResult").get("AccessToken"))
        .setHeader("IDToken", content.get("AuthenticationResult").get("IdToken"))
        .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println (response);
    System.out.println (response.body());
    URL signed = null;
    return new MultiTenancy(auth, signed);
  }

  private MultiTenancy (AuthContent auth, URL signed) {
    this.auth = auth;
    this.host = new HttpHost(signed.getHost(), signed.getPort(), signed.getProtocol());
    this.signed = signed;
  }
  @Override
  public ConnectionFactory clone() {
    return new MultiTenancy(this.auth, this.signed).setAPI(this.api).setIndexName(this.index);
  }
  @Override
  public HttpURLConnection createConnection() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public RestClient createRestClient() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public CredentialsProvider getCredentials() {
    return this.auth.getCredentials();
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
  public boolean isTrustingSelfSigned() {
    return false;
  }
  @Override
  public ConnectionFactory setAPI(String api) {
    this.api = api;
    return this;
  }
  @Override
  public ConnectionFactory setIndexName(String idxName) {
    this.index = idxName;
    return this;
  }
}
