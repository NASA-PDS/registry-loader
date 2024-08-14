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
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.ScrollResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Count;
import gov.nasa.pds.registry.common.Request.Delete;
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
  public Bulk createBulkRequest() {
    return new BulkImpl(this.isServerless);
  }
  @Override
  public Count createCountRequest() {
    return new CountImpl();
  }
  @Override
  public Delete createDelete() {
    return new DeleteImpl();
  }
  @Override
  public DeleteByQuery createDeleteByQuery() {
    return new DBQImpl();
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
  public Response.Bulk performRequest(Bulk request) throws IOException, ResponseException {
    return new Retryable<Response.Bulk,Bulk>() {
      @Override
      public Response.Bulk perform (Bulk arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private Response.Bulk _performRequest(Bulk request) throws IOException, ResponseException {
     return new BulkRespWrap(this.client.bulk(((BulkImpl)request).craftsman.build()));
  }
  @Override
  public long performRequest(Count request) throws IOException, ResponseException {
    return new Retryable<Long,Count>() {
      @Override
      public Long perform (Count arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private long _performRequest(Count request) throws IOException, ResponseException {
    return this.client.count(((CountImpl)request).craftsman.build()).count();
  }
  @Override
  public long performRequest(Delete request) throws IOException, ResponseException {
    return new Retryable<Long,Delete>() {
      @Override
      public Long perform (Delete arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  public long _performRequest(Delete request) throws IOException, ResponseException {
    this.client.delete(((DeleteImpl)request).craftsman.build());
    return 1;
  }
  @Override
  public long performRequest(DeleteByQuery request) throws IOException, ResponseException {
    return new Retryable<Long,DeleteByQuery>() {
      @Override
      public Long perform (DeleteByQuery arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private long _performRequest(DeleteByQuery request) throws IOException, ResponseException {
    SearchResponse<Object> items = this.client.search(((DBQImpl)request).craftsman.size(2).build(), Object.class);
    long deleted = 0, total = items.hits().total().value();
    List<Hit<Object>> hits = items.hits().hits();
    String scrollID =  items.scrollId();
    while (deleted < total) {
      for (Hit<Object> hit : hits) {
        deleted += this.performRequest(this.createDelete().setDocId(hit.id()).setIndex(((DBQImpl)request).index));
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
  public Response.Get performRequest(Get request) throws IOException, ResponseException {
    return new Retryable<Response.Get,Get>() {
      @Override
      public Response.Get perform (Get arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private Response.Get _performRequest(Get request) throws IOException, ResponseException {
    if (request instanceof MGet)
      return new MGetRespWrap(this.client.mget(((MGetImpl)request).craftsman.build(), Object.class));
    return new GetRespWrap(this.client.get(((GetImpl)request).craftsman.build(), Object.class));
  }
  @Override
  public Response.Mapping performRequest(Mapping request) throws IOException, ResponseException {
    return new Retryable<Response.Mapping,Mapping>() {
      @Override
      public Response.Mapping perform (Mapping arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private Response.Mapping _performRequest(Mapping request) throws IOException, ResponseException {
    MappingImpl req = (MappingImpl)request;
    return req.isGet ? new MappingRespImpl(this.client.indices().getMapping(req.craftsman_get.build())) :
      new MappingRespImpl(this.client.indices().putMapping(req.craftsman_set.build()));
  }
  @Override
  public Response.Search performRequest(Search request) throws IOException, ResponseException {
    return new Retryable<Response.Search,Search>() {
      @Override
      public Response.Search perform (Search arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private Response.Search _performRequest(Search request) throws IOException, ResponseException  {
    return new SearchRespWrap(this.client,
        this.client.search(((SearchImpl)request).craftsman.build(), Object.class));
  }
  @Override
  public Response.Settings performRequest(Setting request) throws IOException, ResponseException {
    return new Retryable<Response.Settings,Setting>() {
      @Override
      public Response.Settings perform (Setting arg) throws IOException, ResponseException {
      return _performRequest(arg);
    }}.retry(request);
  }
  private Response.Settings _performRequest(Setting request) throws IOException, ResponseException {
    return new SettingRespImpl(this.client.indices().getSettings(((SettingImpl)request).craftsman.build()));
  }
}
