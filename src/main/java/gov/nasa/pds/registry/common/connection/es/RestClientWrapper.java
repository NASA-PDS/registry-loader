package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import org.elasticsearch.client.RestClientBuilder;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Request.Bulk;
import gov.nasa.pds.registry.common.Request.Method;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.connection.Direct;
import gov.nasa.pds.registry.common.es.dao.DaoUtils;


/**
 * Utility class to build Elasticsearch rest client.
 * 
 * @author karpenko
 */
public class RestClientWrapper implements RestClient
{
    final Direct conFactory;
    final org.elasticsearch.client.RestClient real_client;
    /**
     * Constructor.
     * @throws Exception an exception
     */
    public RestClientWrapper(Direct conFact) throws Exception
    {
      ClientConfigCB clientCB = new ClientConfigCB();
      RequestConfigCB reqCB = new RequestConfigCB();
      RestClientBuilder bld = org.elasticsearch.client.RestClient.builder(conFact.getHost());  
      clientCB.setCredProvider(conFact.getCredentials());
      clientCB.setTrustSelfSignedCert(conFact.isTrustingSelfSigned());
      bld.setHttpClientConfigCallback(clientCB);
      bld.setRequestConfigCallback(reqCB);
      this.conFactory = conFact;
      this.real_client = bld.build();
    }
    @Override
    public void close() throws IOException {
      this.real_client.close();
    }
    @Override
    public Request createRequest(Method method, String endpoint) {
      return new RequestWrapper(method, endpoint);
    }
    @Override
    public Response performRequest(Request request) throws IOException {
      try {
        return new ResponseWrapper(this.real_client.performRequest(((RequestWrapper)request).real_request));
      } catch (org.elasticsearch.client.ResponseException e) {
        throw new ResponseExceptionWrapper(e);
      }
    }
    @Override
    public Bulk createBulkRequest() throws IOException {
      return new BulkWrapper(this.conFactory);
    }
    @Override
    public Response performRequest(Bulk request) throws IOException {
      ((BulkWrapper)request).writer.close();
      DaoUtils.getLastLine(((BulkWrapper)request).con.getInputStream());
      return null;
    }    
}
