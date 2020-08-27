package tt;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;


public class TestRestClient
{

    public static void main(String[] args) throws Exception
    {
        HttpHost host = EsClientBuilder.parseUrl("my-host");
        System.out.println(host);
        
        RestClient client = EsClientBuilder.createClient("localhost");
        client.close();
    }

}
