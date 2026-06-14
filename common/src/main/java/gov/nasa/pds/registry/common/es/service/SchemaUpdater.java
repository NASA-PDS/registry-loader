package gov.nasa.pds.registry.common.es.service;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.dd.LddException;
import gov.nasa.pds.registry.common.dd.LddUtils;
import gov.nasa.pds.registry.common.util.ExceptionUtils;
import gov.nasa.pds.registry.common.util.file.FileDownloader;
import gov.nasa.pds.registry.common.util.Tuple;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;
import gov.nasa.pds.registry.common.meta.OpsFields;


/**
 * Update Elasticsearch schema and LDDs
 * @author karpenko
 */
public class SchemaUpdater
{
    // Cached domain redirects: if a non-pds.nasa.gov host fails but its pds.nasa.gov
    // mirror succeeds, future requests from that host go directly to pds.nasa.gov.
    private static final Map<String, String> domainRedirects = new ConcurrentHashMap<>();
    private Logger log;
    private FileDownloader fileDownloader;
    private JsonLddLoader lddLoader;

    private DataDictionaryDao ddDao;
    private SchemaDao schemaDao;
    
    final private String index;
    private boolean forceLoad = false;

    public void setForceLoad(boolean forceLoad) {
        this.forceLoad = forceLoad;
    }

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
                catch(LddException ex)
                {
                    throw ex;
                }
                catch(Exception ex)
                {
                    log.error("Could not update LDD for namespace '" + prefix + "' at URI " + uri
                        + ": " + ExceptionUtils.getMessage(ex)
                        + ". Harvesting will continue with available field definitions.");
                }
            }
        }
        
        // Update schema: resolve ops: fields from built-in map, all others from the -dd index
        if(fields != null && !fields.isEmpty())
        {
            List<Tuple> newFields = new ArrayList<>();
            Set<String> ddFields = new HashSet<>();
            for(String field : fields)
            {
                String opsType = OpsFields.FIELD_TYPES.get(field);
                if(opsType != null)
                {
                    newFields.add(new Tuple(field, opsType));
                }
                else
                {
                    ddFields.add(field);
                }
            }
            if(!ddFields.isEmpty())
            {
                try
                {
                    List<Tuple> ddTypes = ddDao.getDataTypes(ddFields);
                    if(ddTypes != null)
                    {
                        newFields.addAll(ddTypes);
                    }
                }
                catch(DataTypeNotFoundException ex)
                {
                    if(!forceLoad)
                    {
                        for(String f : ex.getMissingFields())
                        {
                            log.error("Could not find the data type for the field {}", f);
                        }
                        throw ex;
                    }
                    log.warn("Force mode: could not find data types for fields {} - these fields will not be indexed or searchable. Product will still be ingested.", ex.getMissingFields());
                    if(!ex.getFoundTypes().isEmpty())
                    {
                        newFields.addAll(ex.getFoundTypes());
                    }
                }
            }
            if(!newFields.isEmpty())
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

        String jsonUrl = applyDomainRedirect(getJsonUrl(uri));

        int idx = jsonUrl.lastIndexOf('/');
        if(idx < 0)
        {
            throw new LddException("Invalid schema URI." + uri);
        }
        String schemaFileName = jsonUrl.substring(idx + 1);

        LddVersions lddInfo;
        try
        {
            lddInfo = ddDao.getLddInfo(prefix);
        }
        catch(RuntimeException ex)
        {
            throw ex;
        }
        catch(IOException ex)
        {
            throw new LddException("Failed to query registry for existing LDD info for namespace '"
                + prefix + "': " + ExceptionUtils.getMessage(ex), ex);
        }

        if(lddInfo.files.contains(schemaFileName))
        {
            return;
        }

        // Download LDD
        File lddFile = createLddTempFile(prefix);

        try
        {
            boolean downloaded = downloadWithFallback(jsonUrl, lddFile, prefix);
            if(downloaded)
            {
                lddLoader.load(lddFile, schemaFileName, prefix);
            }
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            log.error("Interrupted while downloading or loading LDD for namespace '" + prefix + "' from " + jsonUrl);
            handleDownloadFailure(prefix, lddInfo);
        }
        catch(Exception ex)
        {
            log.error("Failed to download or load LDD for namespace '" + prefix + "' from " + jsonUrl
                + ": " + ExceptionUtils.getMessage(ex));
            handleDownloadFailure(prefix, lddInfo);
        }
        finally
        {
            lddFile.delete();
        }
    }


    /**
     * Attempts to download {@code jsonUrl} to {@code dest}. On failure, tries the pds.nasa.gov
     * mirror and caches the domain redirect for future calls. Returns true if a download succeeded.
     * Throws on {@link InterruptedException} or if both attempts throw a non-interrupt exception.
     */
    private boolean downloadWithFallback(String jsonUrl, File dest, String prefix)
        throws Exception
    {
        try
        {
            return fileDownloader.download(jsonUrl, dest);
        }
        catch(InterruptedException ex)
        {
            throw ex;
        }
        catch(Exception primaryEx)
        {
            String fallbackUrl = LddUrlUtils.toPdsNasaGovUrl(jsonUrl);
            if(fallbackUrl == null)
            {
                throw primaryEx;
            }

            log.warn("Failed to download LDD from " + jsonUrl + "; trying pds.nasa.gov mirror: " + fallbackUrl);
            try
            {
                boolean downloaded = fileDownloader.download(fallbackUrl, dest);
                if(downloaded)
                {
                    String originalHost = new URL(jsonUrl).getHost();
                    domainRedirects.put(originalHost, LddUrlUtils.PDS_NASA_GOV);
                    log.info("Caching domain redirect: " + originalHost + " -> " + LddUrlUtils.PDS_NASA_GOV);
                }
                return downloaded;
            }
            catch(InterruptedException ie)
            {
                throw ie;
            }
            catch(Exception fallbackEx)
            {
                log.debug("pds.nasa.gov mirror also failed: " + fallbackEx.getMessage());
                throw primaryEx;
            }
        }
    }


    private void handleDownloadFailure(String prefix, LddVersions lddInfo) throws LddException
    {
        if(lddInfo.isEmpty())
        {
            if(!forceLoad)
            {
                throw new LddException("No previously loaded LDD found for namespace '"
                    + prefix + "'. Cannot load products with fields from this namespace.");
            }
            log.warn("Force mode: no LDD found for namespace '" + prefix
                + "'. Fields from this namespace will not be indexed.");
        }
        else
        {
            log.warn("Will use previously loaded field definitions for namespace '" + prefix
                + "' from " + lddInfo.files);
        }
    }


    private String getJsonUrl(String uri)
    {
        if(uri.endsWith(".xsd"))
        {
            return uri.substring(0, uri.length()-3) + "JSON";
        }
        else
        {
            throw new IllegalArgumentException("Invalid schema URI - does not end with '.xsd': " + uri);
        }
    }


    /**
     * Apply any cached domain redirect to a URL. If the URL's host has a known
     * redirect (e.g. isda.issdc.gov.in → pds.nasa.gov), return the rewritten URL.
     */
    /**
     * Creates a temp file for LDD download. Tries POSIX permissions first (rw-------); falls back to
     * a plain temp file on non-POSIX filesystems (e.g. Windows NTFS) that throw
     * UnsupportedOperationException or IOException when POSIX attributes are applied.
     */
    public static File createLddTempFile(String prefix) throws LddException
    {
        try
        {
            return Files.createTempFile("LDD-", ".JSON",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))).toFile();
        }
        catch(UnsupportedOperationException | IOException ex)
        {
            try
            {
                return File.createTempFile("LDD-", ".JSON");
            }
            catch(IOException fallbackEx)
            {
                fallbackEx.addSuppressed(ex);
                throw new LddException("Failed to create temp file for LDD download for namespace '"
                    + prefix + "': " + ExceptionUtils.getMessage(fallbackEx), fallbackEx);
            }
        }
    }


    private String applyDomainRedirect(String url)
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

