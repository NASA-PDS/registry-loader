package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Count;
import gov.nasa.pds.registry.common.Request.DeleteByQuery;
import gov.nasa.pds.registry.common.Request.Get;
import gov.nasa.pds.registry.common.Request.MGet;
import gov.nasa.pds.registry.common.Request.Mapping;
import gov.nasa.pds.registry.common.Request.Search;
import gov.nasa.pds.registry.common.Request.Setting;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;

public class RestClientWrapper implements RestClient {
  final private boolean isServerless;
  final private OpenSearchClient client;
  final private SdkHttpClient httpClient;
  public RestClientWrapper(ConnectionFactory conFact, boolean isServerless) {
    this.httpClient = ApacheHttpClient.builder().build();
    this.isServerless = isServerless;
    if (isServerless) {
      this.client = new OpenSearchClient(
          new AwsSdk2Transport(
              httpClient,
              conFact.getHostName(),
              "aoss",
              Region.US_WEST_2, // signing service region that we should probably get from host name??
              AwsSdk2TransportOptions.builder().build()
              )
          );
    } else {
      OpenSearchClient localClient = null;
      try {
        SSLContext sslcontext = SSLContextBuilder
            .create()
            .loadTrustMaterial((chains, authType) -> true)
            .build();
        final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(conFact.getHost5());
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
          final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(sslcontext)
            .build();
          final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder
            .create()
            .setTlsStrategy(tlsStrategy)
            .build();
          return httpClientBuilder
            .setDefaultCredentialsProvider(conFact.getCredentials5())
            .setConnectionManager(connectionManager);
        });
        localClient = new OpenSearchClient(builder.build());
      } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      finally {
        this.client = localClient;
      }
    }
  }
  @Override
  public void close() throws IOException {
    this.client.shutdown();
    this.httpClient.close();
  }
  @Override
  public Bulk createBulkRequest() {
    return new BulkImpl(this.isServerless);
  }
  @Override
  public Count createCountRequest() {
    return new CountImpl();
  }
  @Override
  public Get createGetRequest() {
    return new GetImpl();
  }
  @Override
  public Mapping createMappingRequest() {
    return new MappingImpl();
  }
  @Override
  public MGet createMGetRequest() {
    return new MGetImpl();
  }
  @Override
  public Search createSearchRequest() {
    return new SearchImpl();
  }
  @Override
  public Setting createSettingRequest() {
    return new SettingImpl();
  }
  @Override
  public Response.CreatedIndex create(String indexName, String configAsJson) throws IOException, ResponseException {
    return new CreateIndexRespWrap(this.client.indices().create(CreateIndexConfigWrap.update(new CreateIndexRequest.Builder(), configAsJson).index(indexName).build()));
  }
  @Override
  public void delete(String indexName) throws IOException, ResponseException {
    this.client.indices().delete(new DeleteIndexRequest.Builder().index(indexName).build());
  }
  @Override
  public boolean exists(String indexName) throws IOException, ResponseException {
    return this.client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
  }
  @Override
  public Response.Bulk performRequest(Bulk request) throws IOException, ResponseException {
     return new BulkRespWrap(this.client.bulk(((BulkImpl)request).craftsman.build()));
  }
  @Override
  public long performRequest(Count request) throws IOException, ResponseException {
    return this.client.count(((CountImpl)request).craftsman.build()).count();
  }
  @Override
  public Response.Get performRequest(Get request) throws IOException, ResponseException {
    if (request instanceof MGet)
      return new MGetRespWrap(this.client.mget(((MGetImpl)request).craftsman.build(), Object.class));
    return new GetRespWrap(this.client.get(((GetImpl)request).craftsman.build(), Object.class));
  }
  @Override
  public Response.Mapping performRequest(Mapping request) throws IOException, ResponseException {
    MappingImpl req = (MappingImpl)request;
    return req.isGet ? new MappingRespImpl(this.client.indices().getMapping(req.craftsman_get.build())) :
      new MappingRespImpl(this.client.indices().putMapping(req.craftsman_set.build()));
  }
  @Override
  public Response.Search performRequest(Search request) throws IOException, ResponseException {
    return new SearchRespWrap(this.client.search(((SearchImpl)request).craftsman.build(), Object.class));
  }
  @Override
  public Response.Settings performRequest(Setting request) throws IOException, ResponseException {
    return new SettingRespImpl(this.client.indices().getSettings(((SettingImpl)request).craftsman.build()));
  }
  @Override
  public DeleteByQuery createDeleteByQuery() {
    return new DBQImpl();
  }
  @Override
  public long performRequest(DeleteByQuery request) throws IOException, ResponseException {
    return this.client.deleteByQuery(((DBQImpl)request).craftsman.build()).deleted();
  }
}
