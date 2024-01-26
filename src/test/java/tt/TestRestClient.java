package tt;

import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.connection.es.EsClientFactory;


public class TestRestClient
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsClientFactory.createRestClient(EstablishConnectionFactory.directly("localhost"));
        client.close();
    }

}
