package gov.nasa.pds.registry.common.connection.es;


import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.common.ConnectionFactory;


/**
 * A factory class to create Elasticsearch Rest client instances.
 * 
 * @author karpenko
 */
class EsClientFactory
{
    /**
     * Create Elasticsearch rest client.
     * @param esUrl Elasticsearch URL, e.g., "http://localhost:9200"
     * @param authPath Path to authentication configuration file.
     * @return Elasticsearch rest client instance.
     * @throws Exception an exception
     */
    public static RestClient createRestClient(ConnectionFactory conFact) throws Exception
    {
        EsRestClientBld bld = new EsRestClientBld(conFact);
        return bld.build();
    }

}
