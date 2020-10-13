package gov.nasa.pds.registry.mgr.es.client;

import java.util.Properties;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.PropUtils;

public class EsClientFactory
{
    public static RestClient createRestClient(String esUrl, String authPath) throws Exception
    {
        EsRestClientBld bld = new EsRestClientBld(esUrl);
        
        if(authPath != null)
        {
            Properties props = PropUtils.loadProps(authPath);
            bld.configureAuth(props);
        }
        
        return bld.build();
    }

}
