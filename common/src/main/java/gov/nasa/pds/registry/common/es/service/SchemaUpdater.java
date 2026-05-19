package gov.nasa.pds.registry.common.es.service;


import java.io.File;
import java.net.URL;
import java.util.HashMap;
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
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;


/**
 * Update Elasticsearch schema and LDDs
 * @author karpenko
 */
public class SchemaUpdater
{
    // Cached domain redirects: if a non-pds.nasa.gov host fails but its pds.nasa.gov
    // mirror succeeds, future requests from that host go directly to pds.nasa.gov.
    static final Map<String, String> domainRedirects = new HashMap<>();

    private Logger log;
    private FileDownloader fileDownloader;
    private JsonLddLoader lddLoader;

    private DataDictionaryDao ddDao;
    private SchemaDao schemaDao;
    
    final private String index;
    
    /**
     * Constructor
     * @param cfg Registry (Elasticsearch) configuration
     * @throws Exception
     */
    public SchemaUpdater(ConnectionFactory conFact, DataDictionaryDao ddDao, SchemaDao schemaDao) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        this.ddDao = ddDao;
        this.schemaDao = schemaDao;
        
        fileDownloader = new FileDownloader(true);
        
        lddLoader = new JsonLddLoader(ddDao, conFact);
        lddLoader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile("HARVEST_HOME"));
        this.index = conFact.getIndexName();
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
            List<Tuple> newFields = ddDao.getDataTypes(fields);
            if(newFields != null)
            {
                schemaDao.updateSchema(newFields);
                log.debug("Updated " + newFields.size() + " fields in OpenSearch mapping for index " + this.index);
            }
        }
    }


    private void updateLdd(String uri, String prefix) throws Exception
    {
        if(uri == null || uri.isEmpty()) return;
        if(prefix == null || prefix.isEmpty()) return;

        log.info("Updating '" + prefix  + "' LDD. Schema location: " + uri);

        // Get JSON schema URL from XSD URL, applying any cached domain redirect
        String jsonUrl = applyDomainRedirect(getJsonUrl(uri));

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
            return;
        }

        // Download LDD
        File lddFile = File.createTempFile("LDD-", ".JSON");

        try
        {
            boolean downloaded = false;
            try
            {
                downloaded = fileDownloader.download(jsonUrl, lddFile);
            }
            catch(Exception ex)
            {
                String fallbackUrl = LddUrlUtils.toPdsNasaGovUrl(jsonUrl);
                if(fallbackUrl != null)
                {
                    log.warn("Failed to download LDD from " + jsonUrl + "; trying pds.nasa.gov mirror: " + fallbackUrl);
                    downloaded = fileDownloader.download(fallbackUrl, lddFile);
                    String originalHost = new URL(jsonUrl).getHost();
                    domainRedirects.put(originalHost, LddUrlUtils.PDS_NASA_GOV);
                    log.info("Caching domain redirect: " + originalHost + " -> " + LddUrlUtils.PDS_NASA_GOV);
                    jsonUrl = fallbackUrl;
                }
                else
                {
                    throw ex;
                }
            }
            if(downloaded)
            {
                lddLoader.load(lddFile, schemaFileName, prefix);
            }
        }
        catch(Exception ex)
        {
            log.error("Failed to download or load LDD for namespace '" + prefix + "' from " + jsonUrl + ": " + ExceptionUtils.getMessage(ex));
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
     * Apply any cached domain redirect to a URL. If the URL's host has a known
     * redirect (e.g. isda.issdc.gov.in → pds.nasa.gov), return the rewritten URL.
     */
    private String applyDomainRedirect(String url) throws Exception
    {
        try
        {
            String host = new URL(url).getHost();
            if(domainRedirects.containsKey(host))
            {
                String redirected = LddUrlUtils.toPdsNasaGovUrl(url);
                if(redirected != null)
                {
                    log.debug("Redirecting " + url + " to " + redirected + " (cached domain redirect)");
                    return redirected;
                }
            }
        }
        catch(Exception e)
        {
            // fall through and return original
        }
        return url;
    }


}

