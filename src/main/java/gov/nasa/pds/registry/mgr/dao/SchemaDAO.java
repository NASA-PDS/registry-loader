package gov.nasa.pds.registry.mgr.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.mgr.util.Tuple;


public class SchemaDAO
{
    private RestClient client;
    
    
    public SchemaDAO(RestClient client)
    {
        this.client = client;
    }
    
    
    public Set<String> getFieldNames(String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    public boolean indexExists(String indexName) throws Exception
    {
        Request req = new Request("HEAD", "/" + indexName);
        Response resp = client.performRequest(req);
        return resp.getStatusLine().getStatusCode() == 200;
    }


    public void updateMappings(String indexName, Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return;
        
        List<Tuple> fields = getDataTypes(indexName, ids);
        SchemaRequestBld bld = new SchemaRequestBld();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
    }
    
    
    public List<Tuple> getDataTypes(String indexName, Collection<String> ids) throws Exception
    {
        if(indexName == null) throw new IllegalArgumentException("Index name is null");

        List<Tuple> results = new ArrayList<>();
        if(ids == null || ids.isEmpty()) return results;
        
        // Create request
        indexName = indexName + "-dd";
        Request req = new Request("GET", "/" + indexName + "/_mget?_source=es_data_type");
        
        // Create request body
        SchemaRequestBld bld = new SchemaRequestBld();
        String json = bld.createMgetRequest(ids);
        req.setJsonEntity(json);
        
        // Call ES
        Response resp = client.performRequest(req);
        MgetParser parser = new MgetParser();
        List<MgetParser.Record> records = parser.parse(resp.getEntity());
        
        for(MgetParser.Record rec: records)
        {
            if(rec.found)
            {
                results.add(new Tuple(rec.id, rec.esDataType));
            }
            else
            {
                handleMissingDataTypeMapping(rec.id, results);
            }
        }
        
        return results;
    }
    
    
    private void handleMissingDataTypeMapping(String fieldId, List<Tuple> results) throws Exception
    {
        if(fieldId.startsWith("ref_lid_") || fieldId.startsWith("ref_lidvid_") 
                || fieldId.endsWith("_Area"))
        {
            results.add(new Tuple(fieldId, "keyword"));
        }
        else
        {
            throw new Exception("Could not find datatype for field '" + fieldId + "'.\n" 
                    + "See 'https://nasa-pds.github.io/pds-registry-app/operate/common-ops.html#Load' for more information.");
        }
        
    }
}
