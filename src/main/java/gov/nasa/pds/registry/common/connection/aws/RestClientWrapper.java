package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Method;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import gov.nasa.pds.registry.common.Response;
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
  public Request createRequest(Method method, String endpoint) {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Request request) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Bulk createBulkRequest() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Response performRequest(Bulk request) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
