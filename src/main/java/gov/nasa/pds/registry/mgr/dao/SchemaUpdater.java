package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.CloseUtils;


/**
 * Updates Elasticsearch schema by calling Elasticsearch schema API  
 * @author karpenko
 */
public class SchemaUpdater
{
    private String indexName;
    private SchemaDAO dao;
    
    private Set<String> esFieldNames;
    
    private Set<String> batch;
    private int totalCount;
    private int batchSize = 100;
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public SchemaUpdater(RestClient client, String indexName) throws Exception
    {
        this.indexName = indexName;
        this.dao = new SchemaDAO(client);
        
        // Get a list of existing field names from Elasticsearch
        this.esFieldNames = dao.getFieldNames(indexName);
        
        this.batch = new TreeSet<>();
    }

    
    public void updateSchema(File file) throws Exception
    {
        List<String> newFields = getNewFields(file);
        updateSchema(newFields);
    }
    
    
    /**
     * Add fields from data dictionary to Elasticsearch schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSchema(List<String> newFields) throws Exception
    {
        totalCount = 0;
        batch.clear();

        for(String newField: newFields)
        {
            addField(newField);
        }

        finish();
        System.out.println("Updated " + totalCount + " fields");
    }
    
    
    private void addField(String name) throws Exception
    {
        if(esFieldNames.contains(name)) return;
        
        // Add field request to the batch
        batch.add(name);
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            dao.updateMappings(indexName, batch);
            batch.clear();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        dao.updateMappings(indexName, batch);
    }
    
    
    private static List<String> getNewFields(File file) throws Exception
    {
        List<String> fields = new ArrayList<>();
        
        BufferedReader rd = new BufferedReader(new FileReader(file));
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.length() == 0) continue;
                fields.add(line);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
        
        return fields;
    }

    
}
