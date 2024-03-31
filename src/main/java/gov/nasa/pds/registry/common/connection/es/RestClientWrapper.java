package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClientBuilder;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Count;
import gov.nasa.pds.registry.common.Request.Get;
import gov.nasa.pds.registry.common.Request.Mapping;
import gov.nasa.pds.registry.common.Request.MGet;
import gov.nasa.pds.registry.common.Request.Search;
import gov.nasa.pds.registry.common.Request.Setting;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;


/**
 * Utility class to build Elasticsearch rest client.
 * 
 * @author karpenko
 */
public class RestClientWrapper implements RestClient
{
    final org.elasticsearch.client.RestClient real_client;
    /**
     * Constructor.
     * @throws Exception an exception
     */
    public RestClientWrapper(ConnectionFactory conFact) throws Exception
    {
      ClientConfigCB clientCB = new ClientConfigCB();
      RequestConfigCB reqCB = new RequestConfigCB();
      RestClientBuilder bld = org.elasticsearch.client.RestClient.builder(conFact.getHost());  
      clientCB.setCredProvider(conFact.getCredentials());
      clientCB.setTrustSelfSignedCert(conFact.isTrustingSelfSigned());
      bld.setHttpClientConfigCallback(clientCB);
      bld.setRequestConfigCallback(reqCB);
      this.real_client = bld.build();
    }
    private Response performRequest(String endpoint, String json, String method) throws IOException,ResponseException {
      try {
        Request request = new Request(method, endpoint);
        if (json != null) request.setJsonEntity(json);
        return new ResponseWrapper(this.real_client.performRequest(request));
      } catch (org.elasticsearch.client.ResponseException e) {
        throw new ResponseExceptionWrapper(e);
      }
    }
    @Override
    public void close() throws IOException {
      this.real_client.close();
    }
    @Override
    public Bulk createBulkRequest() {
      return new BulkImpl();
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
      return new GetImpl();
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
    public Response create (String indexName, String configAsJSON) throws IOException,ResponseException {
      return this.performRequest("/" + indexName, configAsJSON, "PUT");
    }
    @Override
    public Response delete (String indexName) throws IOException,ResponseException {
      return this.performRequest(indexName, null, "DELETE");
    }
    @Override
    public Response exists (String indexName) throws IOException,ResponseException {
      return this.performRequest ("/" + indexName, null, "HEAD");
    }
    @Override
    public Response performRequest(Bulk request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), ((BulkImpl)request).json, "POST");
    }
    @Override
    public Response performRequest(Count request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), null, "GET");
    }
    @Override
    public Response performRequest(Get request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), ((GetImpl)request).json, "GET");
    }
    @Override
    public Response performRequest(Mapping request) throws IOException,ResponseException {
      String json = ((MappingImpl)request).json;
      return this.performRequest(request.toString(), json, json == null ? "GET" : "PUT");
    }
    @Override
    public Response performRequest(Search request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), ((SearchImpl)request).json, "GET");
    }
    @Override
    public Response performRequest(Setting request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), null, "GET");
    }
}
