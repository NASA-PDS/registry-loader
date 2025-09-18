package gov.nasa.pds.registry.common.es.dao.schema;

import java.util.List;
import java.util.Set;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.util.Tuple;


/**
 * Elasticsearch schema DAO (Data Access Object).
 * This class provides methods to read and update Elasticsearch schema.
 * 
 * @author karpenko
 */
public class SchemaDao
{
    private RestClient client;
    private String indexName;
    
    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     */
    public SchemaDao(RestClient client, String indexName)
    {
        this.client = client;
        this.indexName = indexName;
    }
    
    
    /**
     * Call Elasticsearch "mappings" API to get a list of field names.
     * @return a collection of field names
     * @throws Exception an exception
     */
    public Set<String> getFieldNames() throws Exception
    {
        Request.Mapping req = client.createMappingRequest().setIndex(indexName);
        return client.performRequest(req).fieldNames();
    }
    
    
    /**
     * Add new fields to Elasticsearch schema.
     * @param fields A list of fields to add. Each field tuple has a name and a data type.
     * @throws Exception an exception
     */
    public void updateSchema(List<Tuple> fields) throws Exception
    {
        if(fields == null || fields.isEmpty()) return;
        
        Request.Mapping req = client.createMappingRequest()
            .buildUpdateFieldSchema(fields)
            .setIndex(this.indexName);
        client.performRequest(req);
    }

}
