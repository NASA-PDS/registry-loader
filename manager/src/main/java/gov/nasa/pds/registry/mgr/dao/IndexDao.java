package gov.nasa.pds.registry.mgr.dao;

import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.RestClient;


/**
 * Data Access Object (DAO) to work with Elasticsearch indices.
 *  
 * @author karpenko
 */
public class IndexDao
{
    private RestClient client;
    
    /**
     * Constructor
     * @param client Elasticsearch client
     */
    public IndexDao(RestClient client)
    {
        this.client = client;
    }
    
    /**
     * Check if Elasticsearch index exists
     * @param indexName Elasticsearch index name
     * @return true if the index exists
     * @throws Exception an exception
     */
    public boolean indexExists(String indexName) throws Exception
    {
        return client.exists (indexName);
    }

    
    /**
     * Get index settings (number of shards and replicas).
     * @param indexName Elasticsearch index name
     * @return index settings
     * @throws Exception an exception
     */
    public IndexSettings getIndexSettings(String indexName) throws Exception
    {
        Request.Setting req = client.createSettingRequest().setIndex(indexName);
        return new IndexSettings(client.performRequest(req));
    }
}
