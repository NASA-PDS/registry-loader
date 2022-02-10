package gov.nasa.pds.registry.mgr.dao.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.common.util.ExceptionUtils;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.mgr.dao.dd.LddVersions;
import gov.nasa.pds.registry.mgr.dd.JsonLddLoader;
import gov.nasa.pds.registry.mgr.dd.LddUtils;
import gov.nasa.pds.registry.mgr.util.Tuple;
import gov.nasa.pds.registry.mgr.util.file.FileDownloader;


/**
 * This class adds new fields to Elasticsearch "registry" index 
 * by calling Elasticsearch schema API. 
 * 
 * The list of field names is read from a file. For all fields not in 
 * current "registry" schema, the data type is looked up in data dictionary 
 * index ("registry-dd").
 * 
 * If a field definition is not available in the data dictionary index,
 * the latest version of LDD will be downloaded if needed.
 * 
 * @author karpenko
 */
public class SchemaUpdater
{
    private Logger log;

    private List<String> batch;
    private int totalCount;
    private int batchSize = 100;
    
    private FileDownloader fileDownloader = new FileDownloader(true);
    private JsonLddLoader lddLoader;
    
    private boolean fixMissingFDs = false;
    
    /**
     * Constructor 
     * @param cfg Registry (Elasticsearch) configuration parameters
     * @throws Exception an exception
     */
    public SchemaUpdater(RegistryCfg cfg, boolean fixMissingFDs) throws Exception
    {
        this.fixMissingFDs = fixMissingFDs;
        
        log = LogManager.getLogger(this.getClass());
        
        lddLoader = new JsonLddLoader(cfg.url, cfg.indexName, cfg.authFile);
        lddLoader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile());
        
        this.batch = new ArrayList<>();
    }

    
    /**
     * Update LDDs and Schema
     * @param dir Harvest output directory
     * @throws Exception an exception
     */
    public void updateLddsAndSchema(File dir) throws Exception
    {
        updateLdds(new File(dir, "missing_xsds.txt"));
        updateSchema(new File(dir, "missing_fields.txt"));
    }
    
    /**
     * Add new fields to Elasticsearch "registry" index. 
     * @param file A file with a list of fields to add.
     * @throws Exception an exception
     */
    public void updateSchema(File file) throws Exception
    {
        log.info("Updating schema with fields from " + file.getAbsolutePath());
        
        List<String> newFields = getNewFields(file);

        totalCount = 0;
        batch.clear();

        for(String newField: newFields)
        {
            addField(newField);
        }

        finish();
        log.info("Updated " + totalCount + " fields");
    }
    
    
    /**
     * Add one field to a batch.
     * @param name field name
     * @throws Exception an exception
     */
    private void addField(String name) throws Exception
    {
        // Add field request to the batch
        batch.add(name);
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            updateSchema(batch);
            batch.clear();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        updateSchema(batch);
    }
    

    /**
     * Add new fields to Elasticsearch "registry" index. 
     * @param batch A batch of field names to add
     * @throws Exception an exception
     */
    public void updateSchema(List<String> batch) throws Exception
    {
        SchemaDao dao = RegistryManager.getInstance().getSchemaDao();
        List<Tuple> newFields = dao.getDataTypes(batch, fixMissingFDs);
        if(newFields != null)
        {
            dao.updateSchema(newFields);
        }
    }
    
    
    /**
     * Update LDDs in Elasticsearch data dictionary. 
     * Only update if remote LDD date is after LDD date in Elasticsearch.
     * @param fileName A file with a list of schema locations.
     * @throws Exception an exception
     */
    public void updateLdds(File file) throws Exception
    {
        log.info("Updating LDDs from " + file.getAbsolutePath());

        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(new FileReader(file));
            
            boolean hasErrors = false;
            String line;
            
            while((line = rd.readLine()) != null)
            {
                int idx = line.indexOf(";");
                if(idx <= 0) continue;
                
                String prefix = line.substring(0, idx);
                String uri = line.substring(idx+1);
                
                try
                {
                    updateLdd(uri, prefix);
                }
                catch(Exception ex)
                {
                    hasErrors = true;
                    log.error(ex.getMessage());
                }
            }
            
            if(hasErrors)
            {
                throw new Exception("There were errors updating LDDs.");
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }

    
    private void updateLdd(String uri, String prefix) throws Exception
    {
        if(uri == null || uri.isEmpty()) return;
        if(prefix == null || prefix.isEmpty()) return;

        log.info("Updating '" + prefix  + "' LDD. Schema location: " + uri);
        
        // Get JSON schema URL from XSD URL
        String jsonUrl = LddUtils.getJsonLddUrlFromXsd(uri);

        // Get schema file name
        int idx = jsonUrl.lastIndexOf('/');
        if(idx < 0) 
        {
            throw new Exception("Invalid schema URI." + uri);
        }
        String schemaFileName = jsonUrl.substring(idx+1);
        
        // Get stored LDDs info
        DataDictionaryDao dao = RegistryManager.getInstance().getDataDictionaryDao();
        LddVersions lddInfo = dao.getLddInfo(prefix);

        // LDD already loaded
        if(lddInfo.files.contains(schemaFileName)) 
        {
            log.info("This LDD already loaded.");
            return;
        }

        // Download LDD
        File lddFile = File.createTempFile("LDD-", ".JSON");
        
        try
        {
            fileDownloader.download(jsonUrl, lddFile);
            lddLoader.loadOnly(lddFile, schemaFileName, prefix, lddInfo.lastDate);
        }
        catch(Exception ex)
        {
            log.warn(ExceptionUtils.getMessage(ex));
            if(lddInfo.isEmpty())
            {
                if(fixMissingFDs)
                {
                    log.warn("Will use 'keyword' data type.");
                    return;
                }
                else
                {
                    throw new Exception("The Registry doesn't have LDD for '" + prefix + "'");
                }
            }
            else
            {
                log.warn("Will use field definitions from " + lddInfo.files);
                return;
            }
        }
        finally
        {
            lddFile.delete();
        }
    }

    
    /**
     * Parse a list of field names (fields.txt file) generated by Harvest.
     * @param file A file with the list of field names. One name per row.
     * @return A list of field names
     * @throws Exception an exception
     */
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
