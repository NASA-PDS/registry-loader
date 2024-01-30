package gov.nasa.pds.registry.common.connection.es;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import gov.nasa.pds.registry.common.ConnectionFactory;


/**
 * Utility class to build Elasticsearch rest client.
 * 
 * @author karpenko
 */
class EsRestClientBld
{
    private RestClientBuilder bld;
    private ClientConfigCB clientCB;
    private RequestConfigCB reqCB;
    /**
     * Constructor.
     * @param url Elasticsearch URL, e.g., "http://localhost:9200"
     * @throws Exception an exception
     */
    public EsRestClientBld(ConnectionFactory conFact) throws Exception
    {
        bld = RestClient.builder(conFact.getHost());
        
        clientCB = new ClientConfigCB();
        clientCB.setCredProvider(conFact.getCredentials());
        clientCB.setTrustSelfSignedCert(conFact.isTrustingSelfSigned());
        reqCB = new RequestConfigCB();
    }    
    /**
     * Build the Elasticsearch rest client
     * @return Elasticsearch rest client
     */
    public RestClient build() 
    {
        bld.setHttpClientConfigCallback(clientCB);
        bld.setRequestConfigCallback(reqCB);
        
        return bld.build();
    }    
}
