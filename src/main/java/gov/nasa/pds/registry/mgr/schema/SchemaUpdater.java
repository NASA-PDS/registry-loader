package gov.nasa.pds.registry.mgr.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsSchemaUtils;


/**
 * Updates Elasticsearch schema by calling Elasticsearch API  
 * @author karpenko
 */
public class SchemaUpdater
{
    private Configuration cfg;
    private Pds2EsDataTypeMap dtMap;
    
    private RestClient client;
    private String indexName;
    
    private Set<String> existingFieldNames;
    
    private UpdateSchemaBatch batch;
    private int totalCount;
    private int lastBatchCount;
    private int batchSize = 100;
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public SchemaUpdater(Configuration cfg, RestClient client, String indexName) throws Exception
    {
        this.cfg = cfg;
        this.client = client;
        this.indexName = indexName;
        
        // Load PDS to Solr data type mapping files
        dtMap = loadDataTypeMap();
        
        // Get a list of existing field names from Solr
        this.existingFieldNames = EsSchemaUtils.getFieldNames(client, indexName);
    }


    /**
     * Load PDS to Elasticsearch data type map(s)
     * @return
     * @throws Exception
     */
    private Pds2EsDataTypeMap loadDataTypeMap() throws Exception
    {
        Pds2EsDataTypeMap map = new Pds2EsDataTypeMap();
        if(cfg.dataTypeFiles != null)
        {
            for(File file: cfg.dataTypeFiles)
            {
                map.load(file);
            }
        }
        
        return map;
    }

    
    /**
     * Add fields from data dictionary to Elasticsearch schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSchema(DataDictionary dd) throws Exception
    {
        lastBatchCount = 0;
        totalCount = 0;
        batch = new UpdateSchemaBatch();
        
        Map<String, String> attrId2Type = dd.getAttributeDataTypeMap();
        Set<String> dataTypes = dd.getDataTypes();
        
        for(DDClass ddClass: dd.getClassMap().values())
        {
            // Skip type definitions.
            if(dataTypes.contains(ddClass.nsName)) continue;
            
            // Apply class filters
            if(cfg.includeClasses != null && cfg.includeClasses.size() > 0)
            {
                if(!cfg.includeClasses.contains(ddClass.nsName)) continue;
            }
            if(cfg.excludeClasses != null && cfg.excludeClasses.size() > 0)
            {
                if(cfg.excludeClasses.contains(ddClass.nsName)) continue;
            }

            File customFile = (cfg.customClassGens == null) ? null : cfg.customClassGens.get(ddClass.nsName);
            if(customFile != null)
            {
                addCustomFields(ddClass, customFile);
            }
            else
            {
                addClassAttributes(ddClass, attrId2Type);
            }
        }
        
        finish();
    }
    
    
    private void addCustomFields(DDClass ddClass, File file) throws Exception
    {
        System.out.println("Loading custom generator. Class = " + ddClass.nsName + ", file = " + file.getAbsolutePath());
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(file));
        }
        catch(Exception ex)
        {
            throw new Exception("Could not open custom generator for class '" 
                    + ddClass.nsName + "':  " + file.getAbsolutePath());
        }
        
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                // Skip blank lines and comments
                if(line.isEmpty() || line.startsWith("#")) continue;
                
                // Line format <field name> = <data type>
                String tokens[] = line.split("=");
                if(tokens.length != 2)
                {
                    throw new Exception("Invalid entry: " + line);
                }
                
                String fieldName = tokens[0].trim();
                String fieldType = tokens[1].trim();                
                
                addEsField(fieldName, fieldType);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void addClassAttributes(DDClass ddClass, Map<String, String> attrId2Type) throws Exception
    {
        for(DDAttr attr: ddClass.attributes)
        {
            String pdsDataType = attrId2Type.get(attr.id);
            if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
            
            String fieldName = ddClass.nsName + "." + attr.nsName;
            String solrDataType = dtMap.getEsType(pdsDataType);
            addEsField(fieldName, solrDataType);
        }
    }

    
    private void addEsField(String name, String type) throws Exception
    {
        name = name.replaceAll("\\.", Constants.REPLACE_DOT_WITH);
        
        if(existingFieldNames.contains(name)) return;
        existingFieldNames.add(name);
        
        // Create add field request to the batch
        batch.addField(name, type);
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
            EsSchemaUtils.updateMappings(client, indexName, batch.closeAndGetJson());
            lastBatchCount = totalCount;
            batch = new UpdateSchemaBatch();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        
        System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
        EsSchemaUtils.updateMappings(client, indexName, batch.closeAndGetJson());
        lastBatchCount = totalCount;
    }
    
    
}
