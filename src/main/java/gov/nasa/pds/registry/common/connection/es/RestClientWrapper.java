package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.util.List;
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
    private org.elasticsearch.client.Response performRequest(String endpoint, String json, String method) throws IOException,ResponseException {
      try {
        Request request = new Request(method, endpoint);
        if (json != null) request.setJsonEntity(json);
        return this.real_client.performRequest(request);
      } catch (org.elasticsearch.client.ResponseException e) {
        throw new ResponseExceptionWrapper(e);
      }
    }
    private void printWarnings(org.elasticsearch.client.Response resp) {
      List<String> warnings = resp.getWarnings();
      if(warnings != null)
      {
        for(String warn: warnings)
        {
          System.out.println("[WARN] " + warn);
        }
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
    public Response.CreatedIndex create (String indexName, String configAsJSON) throws IOException,ResponseException {
      this.printWarnings(this.performRequest("/" + indexName, configAsJSON, "PUT"));
      return new ResponseNotImplYet();
    }
    @Override
    public void delete (String indexName) throws IOException,ResponseException {
      this.printWarnings(this.performRequest(indexName, null, "DELETE"));
    }
    @Override
    public boolean exists (String indexName) throws IOException,ResponseException {
      return this.performRequest ("/" + indexName, null, "HEAD").getStatusLine().getStatusCode() == 200;
    }
    @Override
    public Response.Bulk performRequest(Bulk request) throws IOException,ResponseException {
      return new BulkRespImpl(this.performRequest(request.toString(), ((BulkImpl)request).json, "POST"));
    }
    @Override
    public long performRequest(Count request) throws IOException,ResponseException {
      return JsonHelper.findCount(this.performRequest(request.toString(), null, "GET").getEntity());
    }
    @Override
    public Response performRequest(Get request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), ((GetImpl)request).json, "GET");
    }
    @Override
    public Response.Mapping performRequest(Mapping request) throws IOException,ResponseException {
      String index = ((MappingImpl)request).index;
      String json = ((MappingImpl)request).json;
      return new MappingRespImpl(this.performRequest(request.toString(), json, json == null ? "GET" : "PUT"), index);
    }
    @Override
    public Response performRequest(Search request) throws IOException,ResponseException {
      return this.performRequest(request.toString(), ((SearchImpl)request).json, "GET");
    }
    @Override
    public Response.Settings performRequest(Setting request) throws IOException,ResponseException {
      return new SettingsRespImpl(this.performRequest(request.toString(), null, "GET"));
    }
}
