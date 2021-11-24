package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dd.LddLoader;
import gov.nasa.pds.registry.mgr.dd.LddUtils;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.ExceptionUtils;
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

    private SchemaDao dao;

    private Set<String> batch;
    private int totalCount;
    private int batchSize = 100;
    
    private FileDownloader fileDownloader = new FileDownloader(true);
    private LddLoader lddLoader;
    
    /**
     * Constructor 
     * @param client Elasticsearch client
     * @param cfg Registry (Elasticsearch) configuration parameters
     * @throws Exception an exception
     */
    public SchemaUpdater(RestClient client, RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        this.dao = new SchemaDao(client, cfg.indexName);
        
        lddLoader = new LddLoader(cfg.url, cfg.indexName, cfg.authFile);
        lddLoader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile());
        
        this.batch = new TreeSet<>();
    }

    
    /**
     * Add new fields to Elasticsearch "registry" index. 
     * @param file A file with a list of fields to add.
     * @throws Exception an exception
     */
    public void updateSchema(File file) throws Exception
    {
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
    public void updateSchema(Set<String> batch) throws Exception
    {
        DataTypesInfo info = dao.getDataTypes(batch, false);
        if(info.lastMissingField == null) 
        {
            dao.updateSchema(info.newFields);
            return;
        }
        
        // LDDs are up-to-date or LDD list is not available
        //if(!updated) throw new DataTypeNotFoundException(info.lastMissingField);
        
        // LDDs were updated. Reload last batch. Stop (throw exception) on first missing field.
        info = dao.getDataTypes(batch, true);
        dao.updateSchema(info.newFields);
    }
    
    
    /**
     * Update LDDs in Elasticsearch data dictionary. 
     * Only update if remote LDD date is after LDD date in Elasticsearch.
     * @param fileName A file with a list of schema locations.
     * @throws Exception an exception
     */
    public void updateLdds(String fileName) throws Exception
    {
        if(fileName == null) return;
        
        File file = new File(fileName);
        log.info("Updating LDDs from " + file.getAbsolutePath());

        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(new FileReader(file));
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
                    log.error("Could not update LDD. " + ExceptionUtils.getMessage(ex));
                }
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
        String jsonUrl = getJsonUrl(uri);

        // Get schema file name
        int idx = jsonUrl.lastIndexOf('/');
        if(idx < 0) 
        {
            throw new Exception("Invalid schema URI." + uri);
        }
        String schemaFileName = jsonUrl.substring(idx+1);
        
        // Get stored LDDs info
        LddInfo lddInfo = dao.getLddInfo(prefix);

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
            lddLoader.load(lddFile, schemaFileName, prefix, lddInfo.lastDate);
        }
        catch(Exception ex)
        {
            log.error(ExceptionUtils.getMessage(ex));
            if(lddInfo.isEmpty())
            {
                log.warn("Will use 'keyword' data type.");
                return;
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

    
    private String getJsonUrl(String uri) throws Exception
    {
        if(uri.endsWith(".xsd"))
        {
            String jsonUrl = uri.substring(0, uri.length()-3) + "JSON";
            return jsonUrl;
        }
        else
        {
            throw new Exception("Invalid schema URI. URI doesn't end with '.xsd': " + uri);
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

    
    /**
     * Extract file name from a URL
     * @param url a URL
     * @return file name
     */
    private static String getFileNameFromUrl(String url)
    {
        if(url == null) return null;
        
        int idx = url.lastIndexOf('/');
        if(idx < 0) return url;
        
        return url.substring(idx+1);
    }

}
