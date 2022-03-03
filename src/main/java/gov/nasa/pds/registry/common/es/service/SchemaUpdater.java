package gov.nasa.pds.registry.common.es.service;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.dd.LddUtils;
import gov.nasa.pds.registry.common.util.ExceptionUtils;
import gov.nasa.pds.registry.common.util.file.FileDownloader;

import gov.nasa.pds.registry.common.util.Tuple;

import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;


/**
 * Update Elasticsearch schema and LDDs
 * @author karpenko
 */
public class SchemaUpdater
{
    private Logger log;
    private FileDownloader fileDownloader;
    private JsonLddLoader lddLoader;

    private DataDictionaryDao ddDao;
    private SchemaDao schemaDao;
    
    // Use string data type for undefined fields
    private boolean stringForMissing = true;
    
    /**
     * Constructor
     * @param cfg Registry (Elasticsearch) configuration
     * @throws Exception
     */
    public SchemaUpdater(RegistryCfg cfg, DataDictionaryDao ddDao, SchemaDao schemaDao) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        this.ddDao = ddDao;
        this.schemaDao = schemaDao;
        
        fileDownloader = new FileDownloader(true);
        
        lddLoader = new JsonLddLoader(ddDao, cfg.url, cfg.indexName, cfg.authFile);
        lddLoader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile("HARVEST_HOME"));
    }
    
    
    /**
     * Update Elasticsearch schema
     * @param fields fields to add
     * @param xsds XSDs of fields to add
     * @throws Exception an exception
     */
    public void updateSchema(Set<String> fields, Map<String, String> xsds) throws Exception
    {
        // Update LDDs
        if(xsds != null && !xsds.isEmpty()) 
        {
            log.info("Updating LDDs.");

            for(Map.Entry<String, String> xsd: xsds.entrySet())
            {
                String uri = xsd.getKey();
                String prefix = xsd.getValue();
                
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
        
        // Update schema
        if(fields != null && !fields.isEmpty())
        {
            log.info("Updating Elasticsearch schema.");

            List<Tuple> newFields = ddDao.getDataTypes(fields, stringForMissing);
            if(newFields != null)
            {
                schemaDao.updateSchema(newFields);
                log.info("Updated " + newFields.size() + " fields");
            }
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
        LddVersions lddInfo = ddDao.getLddInfo(prefix);

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
            lddLoader.load(lddFile, schemaFileName, prefix);
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

}
