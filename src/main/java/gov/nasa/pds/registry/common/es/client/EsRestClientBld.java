package gov.nasa.pds.registry.common.es.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import gov.nasa.pds.registry.common.util.JavaProps;


public class EsRestClientBld
{
    private RestClientBuilder bld;
    private ClientConfigCB clientCB;
    private RequestConfigCB reqCB;
    
    
    public EsRestClientBld(String url) throws Exception
    {
        HttpHost host = EsUtils.parseEsUrl(url);
        bld = RestClient.builder(host);
        
        clientCB = new ClientConfigCB();
        reqCB = new RequestConfigCB();
    }
    
    
    public RestClient build() 
    {
        bld.setHttpClientConfigCallback(clientCB);
        bld.setRequestConfigCallback(reqCB);
        
        return bld.build();
    }
    
    
    public void configureAuth(JavaProps props) throws Exception
    {
        if(props == null) return;

        // Trust self-signed certificates
        if(Boolean.TRUE.equals(props.getBoolean(ClientConstants.AUTH_TRUST_SELF_SIGNED)))
        {
            clientCB.setTrustSelfSignedCert(true);
        }
        
        // Basic authentication
        String user = props.getProperty("user");
        String pass = props.getProperty("password");
        if(user != null && pass != null)
        {
            clientCB.setUserPass(user, pass);
        }
    }
}
