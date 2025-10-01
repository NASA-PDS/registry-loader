package gov.nasa.pds.registry.common.connection.es;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class to process Elasticsearch response from "search IDs" query.  
 * 
 * @author karpenko
 */
class NonExistingIdsResponse implements SearchResponseParser.Callback
{
    private Set<String> retIds;


    /**
     * Constructor
     * @param ids Product IDs (lidvids) sent to Elasticsearch in "search IDs" query.
     * IDs are copied to internal collection.
     * After processing Elasticsearch response, all IDs existing in Elasticsearch
     * "registry" index will be removed from this internal collection.
     */
    NonExistingIdsResponse(Collection<String> ids)
    {
        retIds = new TreeSet<>(ids);
    }
    
    /**
     * Return collection of product IDs (lidvids) non-existing in Elasticsearch.
     * @return a collection of product IDs (lidvids)
     */
    public Set<String> getIds()
    {
        return retIds;
    }
    
    
    /**
     * This method is called for each record in Elasticsearch response
     */
    @Override
    public void onRecord(String id, Object src)
    {
        retIds.remove(id);
    }
}
