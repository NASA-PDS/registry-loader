package tt;

import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;


public class TestRestClient
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EstablishConnectionFactory.directly("localhost").createRestClient();
        client.close();
    }

}
