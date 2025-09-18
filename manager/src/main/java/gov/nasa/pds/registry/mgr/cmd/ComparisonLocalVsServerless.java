package example;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

public class ComparisonLocalVsServerless {
  /**
   * These variables control all there is to the test
   *
   * authcog : a file containing username/password in properties form authloc : a file containing
   * username/password in properties form clientid : the cognito client ID endpointcog : the
   * endpoint of serverless opensearch endpointlog : the endpoint of local opensearch (docker 2.13.0
   * running on port 9200) gateway : exchange cognito keys for AWS credentials that serverless
   * opensearch likes idp : the endpoint to convert username/password to cognito keys index : random
   * name for test index
   *
   *
   * The authfile is simple and looks like: user = some_user_name password = the password needed to
   * gain access at the endpoint and given user
   */
  final String authcog = "/home/niessner/Projects/PDS/TestData/OSV2/auth-cog.txt";
  final String authloc = "/home/niessner/Projects/PDS/TestData/OSV2/auth.txt";
  final String clientid = "47d9j6ks9un4errq6pnbu0bc1r";
  final String endpointcog = "https://p5qmxrldysl1gy759hqf.us-west-2.aoss.amazonaws.com";
  final String endpointloc = "https://localhost:9200";
  final String gateway = "https://c8u1zk30u5.execute-api.us-west-2.amazonaws.com/dev/credentials";
  final String idp = "https://cognito-idp.us-west-2.amazonaws.com";
  final String index = "niessner_test_comparison_index";
  private HashMap<String, String> doc_a = new HashMap<String, String>();
  private HashMap<String, String> doc_b = new HashMap<String, String>();
  private String password;
  private String username;

  public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException,
      KeyStoreException, IOException, InterruptedException {
    ComparisonLocalVsServerless self = new ComparisonLocalVsServerless();
    self.makedata();
    self.loadauth(self.authloc);
    self.doTest("local: ", self.locally());
    self.loadauth(self.authcog);
    self.doTest("aoss: ", self.serverless());
  }

  private void doTest(String prefix, OpenSearchClient client)
      throws OpenSearchException, IOException {
    if (client.indices().exists(new ExistsRequest.Builder().index(this.index).build()).value()) {
      System.out.println(prefix + "Index already exists so delete it first");
      client.indices().delete(new DeleteIndexRequest.Builder().index(this.index).build());
    }
    if (client.indices().exists(new ExistsRequest.Builder().index(this.index).build()).value()) {
      System.err.println(prefix + "DB not clean. Aborting tests.");
      return;
    }
    client.indices().create(new CreateIndexRequest.Builder().index(this.index).build());
    if (!client.indices().exists(new ExistsRequest.Builder().index(this.index).build()).value()) {
      System.err.println(prefix + "Could not create the index. Aborting tests.");
      return;
    } else {
      System.out.println(prefix + "Created the testing index");
    }
    client.bulk(new BulkRequest.Builder()
        .operations(new BulkOperation.Builder().create(new CreateOperation.Builder<Object>()
            .document(this.doc_a).id(this.doc_a.get("lidvid")).index(this.index).build()).build())
        .operations(new BulkOperation.Builder().create(new CreateOperation.Builder<Object>()
            .document(this.doc_b).id(this.doc_b.get("lidvid")).index(this.index).build()).build())
        .build());
    GetResponse<Object> gr = null;
    gr = client.get(new GetRequest.Builder().id(doc_b.get("lidvid")).sourceIncludes("product_class")
        .index(this.index).build(), Object.class);
    if (gr == null || !gr.id().equals(this.doc_b.get("lidvid"))) {
      System.err.println(prefix + "Could not find document with ID " + this.doc_b.get("lidvid"));
    } else {
      System.out.println(prefix + "Found expected document");
    }
    gr = client.get(new GetRequest.Builder().id(doc_a.get("lidvid")).sourceIncludes("product_class")
        .index(this.index).build(), Object.class);
    if (gr == null || !gr.id().equals(this.doc_a.get("lidvid"))) {
      System.err.println(prefix + "Could not find document with ID " + this.doc_a.get("lidvid"));
    } else {
      System.out.println(prefix + "Found expected document");
    }
    System.out.println(prefix + "Test complete");
  }

  private void loadauth(String authfile) throws IOException {
    Properties props = new Properties();
    FileReader rd = new FileReader(authfile);
    props.load(rd);
    rd.close();
    this.username = props.getProperty("user");
    this.password = props.getProperty("password");
  }

  private OpenSearchClient locally() throws KeyManagementException, NoSuchAlgorithmException,
      KeyStoreException, MalformedURLException {
    SSLContext sslcontext =
        SSLContextBuilder.create().loadTrustMaterial((chains, authType) -> true).build();
    final URL endpoint = new URL(endpointloc);
    final ApacheHttpClient5TransportBuilder builder =
        ApacheHttpClient5TransportBuilder.builder(new org.apache.hc.core5.http.HttpHost(
            endpoint.getProtocol(), endpoint.getHost(), endpoint.getPort()));
    builder.setHttpClientConfigCallback(httpClientBuilder -> {
      final TlsStrategy tlsStrategy =
          ClientTlsStrategyBuilder.create().setSslContext(sslcontext).build();
      final PoolingAsyncClientConnectionManager connectionManager =
          PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();
      org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider creds =
          new org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider();
      creds.setCredentials(
          new org.apache.hc.client5.http.auth.AuthScope(null, null, -1, null, null), // ANY
          new org.apache.hc.client5.http.auth.UsernamePasswordCredentials(this.username,
              this.password.toCharArray()));
      return httpClientBuilder.setDefaultCredentialsProvider(creds)
          .setConnectionManager(connectionManager);
    });
    return new OpenSearchClient(builder.build());
  }

  private void makedata() {
    this.doc_a.put("lidvid", "a:b:c:d:e::1.2");
    this.doc_a.put("product_class", "colon");
    this.doc_a.put("dessert", "apple pie");
    this.doc_b.put("lidvid", "a_b_c_d_e__1.2");
    this.doc_b.put("product_class", "underscore");
    this.doc_b.put("dessert", "cherry pie");
  }

  private OpenSearchClient serverless() throws IOException, InterruptedException {
    boolean expectedContent = true;
    Gson gson = new Gson();
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(idp))
        .POST(BodyPublishers.ofString("{\"AuthFlow\":\"USER_PASSWORD_AUTH\",\"AuthParameters\":{"
            + "\"USERNAME\":\"" + this.username + "\"," + "\"PASSWORD\":\"" + this.password + "\""
            + "},\"ClientId\":\"" + clientid + "\"" + "}"))
        .setHeader("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")
        .setHeader("Content-Type", "application/x-amz-json-1.1").build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    Map<String, Map<String, String>> content;
    Properties awsCreds = new Properties(System.getProperties()); // initialize properties as oracle
                                                                  // suggests
    Type contentType = new TypeToken<Map<String, Map<String, String>>>() {}.getType();
    expectedContent &= response.body().contains("AuthenticationResult");
    expectedContent &= response.body().contains("AccessToken");
    expectedContent &= response.body().contains("ExpiresIn");
    expectedContent &= response.body().contains("IdToken");
    expectedContent &= response.body().contains("RefreshToken");
    expectedContent &= response.body().contains("TokenType");
    expectedContent &= response.body().contains("ChallengeParameters");
    if (!expectedContent) {
      throw new IOException(
          "Received an unexpected response of: " + response.toString() + " ->\n" + response.body());
    }
    content = gson.fromJson(response.body(), contentType);
    client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
    request = HttpRequest.newBuilder().uri(URI.create(gateway)).GET()
        .setHeader("Authorization",
            content.get("AuthenticationResult").get("TokenType") + " "
                + content.get("AuthenticationResult").get("AccessToken"))
        .setHeader("IDToken", content.get("AuthenticationResult").get("IdToken")).build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    if (299 < response.statusCode()) {
      throw new IOException("Could not obtain credentials: " + response.toString());
    }
    expectedContent &= response.body().contains("body");
    expectedContent &= response.body().contains("AccessKeyId");
    expectedContent &= response.body().contains("SecretAccessKey");
    expectedContent &= response.body().contains("SessionToken");
    if (!expectedContent) {
      throw new IOException(
          "Did not find expected credential response from: " + response.toString());
    }
    content = gson.fromJson(response.body(), new TypeToken<Map<String, Object>>() {}.getType());
    content = gson.fromJson("{\"Credentials\":" + content.get("body") + "}", contentType);
    // fill then set system properties as oracle suggests (init happened above)
    awsCreds.setProperty("aws.accessKeyId", content.get("Credentials").get("AccessKeyId"));
    awsCreds.setProperty("aws.secretAccessKey", content.get("Credentials").get("SecretAccessKey"));
    awsCreds.setProperty("aws.sessionToken", content.get("Credentials").get("SessionToken"));
    System.setProperties(awsCreds);
    return new OpenSearchClient(new AwsSdk2Transport(ApacheHttpClient.builder().build(),
        new URL(endpointcog).getHost(), "aoss", Region.US_WEST_2, // signing service region that we
                                                                  // should probably get from host
                                                                  // name??
        AwsSdk2TransportOptions.builder().build()));
  }
}
