package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Count;
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
  final private OpenSearchClient client;
  final private SdkHttpClient httpClient;
  public RestClientWrapper(ConnectionFactory conFact) {
    this.httpClient = ApacheHttpClient.builder().build();
    this.client = new OpenSearchClient(
        new AwsSdk2Transport(
            httpClient,
            conFact.getHostName(),
            "aoss",
            Region.US_WEST_2, // signing service region that we should probably get from host name??
            AwsSdk2TransportOptions.builder().build()
        )
    );    
  }
  @Override
  public void close() throws IOException {
    this.client.shutdown();
    this.httpClient.close();
  }
  @Override
  public Bulk createBulkRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Count createCountRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Get createGetRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Mapping createMappingRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public MGet createMGetRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Search createSearchRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Setting createSettingRequest() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response create(String indexName, String configAsJson)
      throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response delete(String indexName) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response exists(String indexName) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Bulk request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Count request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Get request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Mapping request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Search request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Setting request) throws IOException, ResponseException {
    // TODO Auto-generated method stub
    return null;
  }
}
