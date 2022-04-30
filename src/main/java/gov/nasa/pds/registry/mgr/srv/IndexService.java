package gov.nasa.pds.registry.mgr.srv;

import java.io.File;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.mgr.dao.IndexDao;
import gov.nasa.pds.registry.mgr.dao.RegistryRequestBuilder;

/**
 * A service to work with Elasticsearch indices
 * @author karpenko
 */
public class IndexService
{
    private static final String ERR_CFG = 
            "Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set."; 

    private RestClient client;
    private IndexDao indexDao;
    
    
    /**
     * Constructor
     * @param client Elasticsearch client
     */
    public IndexService(RestClient client)
    {
        this.client = client;
        indexDao = new IndexDao(client);
    }

    
    /**
     * Create Elasticsearch index
     * @param relativeSchemaPath Relative path to Elasticsearch index schema file.
     * The path is relative to $REGISTRY_MANGER_HOME, e.g., "elastic/registry.json"
     * @param indexName Elasticsearch index name
     * @param shards number of shards
     * @param replicas number of replicas
     * @throws Exception
     */
    public void createIndex(String relativeSchemaPath, String indexName, int shards, int replicas) throws Exception
    {
        File schemaFile = getSchemaFile(relativeSchemaPath);
        
        try
        {
            System.out.println("Creating index...");
            System.out.println("   Index: " + indexName);
            System.out.println("  Schema: " + schemaFile.getAbsolutePath());
            System.out.println("  Shards: " + shards);
            System.out.println("Replicas: " + replicas);
            
            // Create request
            Request req = new Request("PUT", "/" + indexName);
            RegistryRequestBuilder bld = new RegistryRequestBuilder();
            String jsonReq = bld.createCreateIndexRequest(schemaFile, shards, replicas);
            req.setJsonEntity(jsonReq);

            // Execute request
            Response resp = client.performRequest(req);
            EsUtils.printWarnings(resp);
            System.out.println("Done");
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
    }

    
    /**
     * Get Elasticsearch schema file by relative path.
     * @param relPath
     * @return
     * @throws Exception
     */
    public File getSchemaFile(String relPath) throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) throw new Exception(ERR_CFG);

        File file = new File(home, relPath);
        if(!file.exists()) throw new Exception("Schema file " + file.getAbsolutePath() + " does not exist");
        
        return file;
    }

    
    /**
     * Get default data dictionary data file.
     * @return
     * @throws Exception
     */
    public File getDataDicFile() throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) 
        {
            throw new Exception(ERR_CFG);
        }
        
        File file = new File(home, "elastic/data-dic-data.jar");
        return file;
    }

    
    /**
     * Delete Elasticsearch index by name
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public void deleteIndex(String indexName) throws Exception
    {
        try
        {
            System.out.println("Deleting index " + indexName);
            
            if(!indexDao.indexExists(indexName)) 
            {
                return;
            }

            // Create request
            Request req = new Request("DELETE", "/" + indexName);

            // Execute request
            Response resp = client.performRequest(req);
            EsUtils.printWarnings(resp);
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
    }

}
