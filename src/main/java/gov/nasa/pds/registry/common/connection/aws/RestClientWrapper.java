package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.CountRequest;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.MgetRequest;
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.ScrollResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetIndicesSettingsRequest;
import org.opensearch.client.opensearch.indices.GetMappingRequest;
import org.opensearch.client.opensearch.indices.PutMappingRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;

public class RestClientWrapper implements RestClient {
  private abstract class Retryable <R,T> {
    abstract public R perform (T arg) throws IOException, ResponseException;
    public R retry (T arg) throws IOException, ResponseException {
      int retries = 0, retry_limit = 3;
      while (true) {
        try { return this.perform(arg); }
        catch (OpenSearchException ose) {
          if (ose.response().status() == 403) {
            retries++;
            if (retries < retry_limit) {
              try { conFact.reconnect(); }
              catch (InterruptedException ie) { throw new RuntimeException ("How did this happen??", ie); }
              client = buildClient();
            } else {
              log.error ("Tried " + retry_limit + " to re-establish connection but cannot.");
              throw ose;
            }
          } else {
            throw ose;
          }
        }
      }
    }
  }
  final private boolean isServerless;
  final private ConnectionFactory conFact;
  final private Logger log;
  final private SdkHttpClient httpClient;
  private OpenSearchClient client;
  public RestClientWrapper(ConnectionFactory conFact, boolean isServerless) {
    this.conFact = conFact;
    this.httpClient = ApacheHttpClient.builder().build();
    this.isServerless = isServerless;
    this.log = LogManager.getLogger(this.getClass());
    this.client = this.buildClient();
  }
  private OpenSearchClient buildClient() {
    OpenSearchClient client = null;
    if (isServerless) {
      client = new OpenSearchClient(
          new AwsSdk2Transport(
              this.httpClient,
              this.conFact.getHostName(),
              "aoss",
              Region.US_WEST_2, // signing service region that we should probably get from host name??
              AwsSdk2TransportOptions.builder().build()
              )
          );
    } else {
      try {
        SSLContext sslcontext = SSLContextBuilder
            .create()
            .loadTrustMaterial((chains, authType) -> true)
            .build();
        final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(this.conFact.getHost5());
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
        client = new OpenSearchClient(builder.build());
      } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return client;
  }
  @Override
  public void close() throws IOException {
    this.client.shutdown();
    this.httpClient.close();
  }
  @Override
  public Response.CreatedIndex create(String indexName, String configAsJson) throws IOException, ResponseException {
    return new CreateIndexRespWrap(this.client.indices().create(CreateIndexConfigWrap.update(new CreateIndexRequest.Builder(), configAsJson).index(indexName).build()));
  }
  @Override
  public Request.Bulk createBulkRequest() {
    return new BulkImpl(this.isServerless);
  }
  @Override
  public Request.Count createCountRequest() {
    return new CountImpl();
  }
  @Override
  public Request.Delete createDelete() {
    return new DeleteImpl();
  }
  @Override
  public Request.DeleteByQuery createDeleteByQuery() {
    return new DBQImpl();
  }
  @Override
  public Request.Get createGetRequest() {
    return new GetImpl();
  }
  @Override
  public Request.Mapping createMappingRequest() {
    return new MappingImpl();
  }
  @Override
  public Request.MGet createMGetRequest() {
    return new MGetImpl();
  }
  @Override
  public Request.Search createSearchRequest() {
    return new SearchImpl();
  }
  @Override
  public Request.Setting createSettingRequest() {
    return new SettingImpl();
  }
  @Override
  public void delete(String indexName) throws IOException, ResponseException {
    new Retryable<Object,String>() {
      @Override
      public Object perform (String arg) throws IOException, ResponseException {
      _delete(arg);
      return null;
    }}.retry(indexName);
  }
  private void _delete(String indexName) throws IOException, ResponseException {
    this.client.indices().delete(new DeleteIndexRequest.Builder().index(indexName).build());
  }
  @Override
  public boolean exists(String indexName) throws IOException, ResponseException {
    return new Retryable<Boolean,String>() {
      @Override
      public Boolean perform (String arg) throws IOException, ResponseException {
      return _exists(arg);
    }}.retry(indexName);
  }
  private boolean _exists(String indexName) throws IOException, ResponseException {
    return this.client.indices().exists(new ExistsRequest.Builder().index(indexName).build()).value();
  }
  @Override
  public Response.Bulk performRequest(Request.Bulk request) throws IOException, ResponseException {
    return new Retryable<Response.Bulk,BulkRequest>() {
      @Override
      public Response.Bulk perform (BulkRequest arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(((BulkImpl)request).craftsman.build());
  }
  private Response.Bulk _performRequest(BulkRequest request) throws IOException, ResponseException {
     return new BulkRespWrap(this.client.bulk(request));
  }
  @Override
  public long performRequest(Request.Count request) throws IOException, ResponseException {
    return new Retryable<Long,CountRequest>() {
      @Override
      public Long perform (CountRequest arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(((CountImpl)request).craftsman.build());
  }
  private long _performRequest(CountRequest request) throws IOException, ResponseException {
    return this.client.count(request).count();
  }
  @Override
  public long performRequest(Request.Delete request) throws IOException, ResponseException {
    return new Retryable<Long,DeleteRequest>() {
      @Override
      public Long perform (DeleteRequest arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(((DeleteImpl)request).craftsman.build());
  }
  public long _performRequest(DeleteRequest request) throws IOException, ResponseException {
    this.client.delete(request);
    return 1;
  }
  @Override
  public long performRequest(Request.DeleteByQuery request) throws IOException, ResponseException {
    return new Retryable<Long,SearchRequest>() {
      @Override
      public Long perform (SearchRequest arg) throws IOException, ResponseException {
      return _performDBQRequest(arg);
    }}.retry(((DBQImpl)request).craftsman.size(2).build());
  }
  private long _performDBQRequest(SearchRequest request) throws IOException, ResponseException {
    SearchResponse<Object> items = this.client.search(request, Object.class);
    long deleted = 0, total = items.hits().total().value();
    List<Hit<Object>> hits = items.hits().hits();
    String scrollID =  items.scrollId();
    while (deleted < total) {
      for (Hit<Object> hit : hits) {
        deleted += this.performRequest(this.createDelete().setDocId(hit.id()).setIndex(request.index().get(0)));
      }
      if (deleted < total) {
        ScrollResponse<Object> page = this.client.scroll(new ScrollRequest.Builder()
            .scroll(new Time.Builder().time("24m").build()).scrollId(scrollID).build(), Object.class);
        hits = page.hits().hits();
        scrollID = page.scrollId(); // docs say it may change
      }
    }
    return total;
  }
  @Override
  public Response.Get performRequest(Request.Get request) throws IOException, ResponseException {
    if (request instanceof Request.MGet) {
      return new Retryable<Response.Get,MgetRequest>() {
        @Override
        public Response.Get perform (MgetRequest arg) throws IOException, ResponseException {
        return new MGetRespWrap(client.mget(arg, Object.class));
      }}.retry(((MGetImpl)request).craftsman.build());
    }
    return new Retryable<Response.Get,GetRequest>() {
      @Override
      public Response.Get perform (GetRequest arg) throws IOException, ResponseException {
        return new GetRespWrap(client.get(arg, Object.class));
    }}.retry(((GetImpl)request).craftsman.build());
  }
  @Override
  public Response.Mapping performRequest(Request.Mapping request) throws IOException, ResponseException {
    MappingImpl req = (MappingImpl)request;
    if (req.isGet) {
      return new Retryable<Response.Mapping,GetMappingRequest>() {
        @Override
        public Response.Mapping perform (GetMappingRequest arg) throws IOException, ResponseException {
        return new MappingRespImpl(client.indices().getMapping(arg));
      }}.retry(req.craftsman_get.build());      
    }
    return new Retryable<Response.Mapping,PutMappingRequest>() {
      @Override
      public Response.Mapping perform (PutMappingRequest arg) throws IOException, ResponseException {
      return new MappingRespImpl(client.indices().putMapping(arg));
    }}.retry(req.craftsman_set.build());      
  }
  @Override
  public Response.Search performRequest(Request.Search request) throws IOException, ResponseException {
    return new Retryable<Response.Search,SearchRequest>() {
      @Override
      public Response.Search perform (SearchRequest arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(((SearchImpl)request).craftsman.build());
  }
  private Response.Search _performRequest(SearchRequest request) throws IOException, ResponseException  {
    return new SearchRespWrap(this.client, this.client.search(request, Object.class));
  }
  @Override
  public Response.Settings performRequest(Request.Setting request) throws IOException, ResponseException {
    return new Retryable<Response.Settings,GetIndicesSettingsRequest>() {
      @Override
      public Response.Settings perform (GetIndicesSettingsRequest arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(((SettingImpl)request).craftsman.build());
  }
  private Response.Settings _performRequest(GetIndicesSettingsRequest request) throws IOException, ResponseException {
    return new SettingRespImpl(this.client.indices().getSettings(request));
  }
}
