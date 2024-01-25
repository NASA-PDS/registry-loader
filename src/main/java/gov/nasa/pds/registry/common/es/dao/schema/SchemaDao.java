package gov.nasa.pds.registry.common.es.dao.schema;

import java.util.List;
import java.util.Set;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

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
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    /**
     * Add new fields to Elasticsearch schema.
     * @param fields A list of fields to add. Each field tuple has a name and a data type.
     * @throws Exception an exception
     */
    public void updateSchema(List<Tuple> fields) throws Exception
    {
        if(fields == null || fields.isEmpty()) return;
        
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        client.performRequest(req);
    }

}
