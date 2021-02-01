package gov.nasa.pds.registry.common.es.client;


import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.common.util.JavaProps;


public class EsClientFactory
{
    public static RestClient createRestClient(String esUrl, String authPath) throws Exception
    {
        EsRestClientBld bld = new EsRestClientBld(esUrl);
        
        if(authPath != null)
        {
            JavaProps props = new JavaProps(authPath);
            bld.configureAuth(props);
        }
        
        return bld.build();
    }

}
