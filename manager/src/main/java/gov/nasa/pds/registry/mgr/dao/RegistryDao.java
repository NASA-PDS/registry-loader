package gov.nasa.pds.registry.mgr.dao;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.stream.JsonWriter;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.util.Tuple;

/**
 * Data access object
 * @author karpenko
 */
public class RegistryDao
{    
    private RestClient client;
    private String indexName;

    private boolean pretty = false;

    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index
     */
    public RegistryDao(RestClient client, String indexName)
    {        
        this.client = client;
        this.indexName = indexName;
    }

    
    /**
     * Generate pretty JSONs for debugging
     * @param b boolean flag
     */
    public void setPretty(boolean b)
    {
        this.pretty = b;
    }
    
    
    /**
     * Get product's alternative IDs by primary key 
     * @param ids primary keys (usually LIDVIDs)
     * @return ID map: key = product primary key (usually LIDVID), value = set of alternate IDs 
     * @throws Exception an exception
     */
    public Map<String, Set<String>> getAlternateIds(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;
                
        Request.Search req = client.createSearchRequest()
            .buildAlternativeIds(ids)
            .setIndex(this.indexName)
            .setPretty(pretty);
        return client.performRequest(req).altIds();
    }
    
    
    /**
     * Update alternate IDs by primary keys
     * @param newIds ID map: key = product primary key (usually LIDVID), 
     * value = additional alternate IDs to be added to existing alternate IDs.
     * @throws Exception an exception
     */
    public void updateAlternateIds(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) return;
        
        Request.Bulk req = client.createBulkRequest()
            .setIndex(this.indexName)
            .setRefresh(Request.Bulk.Refresh.WaitFor);
        for (Tuple t : this.createUpdateAltIdsRequest(newIds)) {
          req.add(t.item1, t.item2);
        }
        client.performRequest(req).logErrors();
    }

    private List<Tuple> createUpdateAltIdsRequest(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) throw new IllegalArgumentException("Missing ids");
        ArrayList<Tuple> updates = new ArrayList<Tuple>();
        // Build NJSON (new-line delimited JSON)
        for(Map.Entry<String, Set<String>> entry: newIds.entrySet())
        {
          Tuple statement = new Tuple();
          // Line 1: Elasticsearch document ID
          statement.item1 = "{ \"update\" : {\"_id\" : \"" + entry.getKey() + "\" } }";
          // Line 2: Data
          statement.item2 = buildUpdateDocJson("alternate_ids", entry.getValue());
          updates.add(statement);
        }
        return updates;
    }
    private String buildUpdateDocJson(String field, Collection<String> values) throws Exception
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = new JsonWriter(out);;

        writer.beginObject();

        writer.name("doc");
        writer.beginObject();
        
        writer.name(field);
        
        writer.beginArray();        
        for(String value: values)
        {
            writer.value(value);
        }
        writer.endArray();
        
        writer.endObject();        
        writer.endObject();
        
        writer.close();
        return out.toString();
    }
}
