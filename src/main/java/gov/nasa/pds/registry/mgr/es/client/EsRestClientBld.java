package gov.nasa.pds.registry.mgr.es.client;

import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.PropUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


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
    
    
    public void configureAuth(Properties props) throws Exception
    {
        if(props == null) return;

        // Trust self-signed certificates
        if(Boolean.TRUE.equals(PropUtils.getBoolean(props, Constants.AUTH_TRUST_SELF_SIGNED)))
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
