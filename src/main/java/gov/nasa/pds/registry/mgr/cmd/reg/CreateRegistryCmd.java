package gov.nasa.pds.registry.mgr.cmd.reg;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class CreateRegistryCmd implements CliCommand
{
    private static final String ERR_CFG = 
            "Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set."; 
    
    private RestClient client;

    
    public CreateRegistryCmd()
    {
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");
        
        int shards = parseShards(cmdLine.getOptionValue("shards", "1"));
        int replicas = parseReplicas(cmdLine.getOptionValue("replicas", "0"));
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println();
        
        client = EsClientFactory.createRestClient(esUrl, authPath);

        try
        {
            // Registry
            createIndex("elastic/registry.json", indexName, shards, replicas);
            System.out.println();
            
            // Collection inventory (product references)
            createIndex("elastic/refs.json", indexName + "-refs", shards, replicas);
            System.out.println();
            
            // Data dictionary
            createIndex("elastic/data-dic.json", indexName + "-dd", 1, replicas);
            // Load data
            DataLoader dl = new DataLoader(esUrl, indexName + "-dd", authPath);
            File zipFile = getDDDataFile();
            dl.loadZippedFile(zipFile, "dd.json");
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    private void createIndex(String relativeSchemaPath, String indexName, int shards, int replicas) throws Exception
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
            EsRequestBuilder bld = new EsRequestBuilder();
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
    

    private int parseShards(String str) throws Exception
    {
        int val = parseInt(str);
        if(val <= 0) throw new Exception("Invalid number of shards: " + str);
        
        return val;
    }
    

    private int parseReplicas(String str) throws Exception
    {
        int val = parseInt(str);
        if(val < 0) throw new Exception("Invalid number of replicas: " + str);
        
        return val;
    }

    
    private int parseInt(String str)
    {
        if(str == null) return 0;
        
        try
        {
            return Integer.parseInt(str);
        }
        catch(Exception ex)
        {
            return -1;
        }
    }
    
    
    private File getSchemaFile(String relPath) throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) throw new Exception(ERR_CFG);

        File file = new File(home, relPath);
        if(!file.exists()) throw new Exception("Schema file " + file.getAbsolutePath() + " does not exist");
        
        return file;
    }
    
    
    private File getDDDataFile() throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) 
        {
            throw new Exception(ERR_CFG);
        }
        
        File file = new File(home, "elastic/data-dic-data.jar");
        return file;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry index");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>         Authentication config file");
        System.out.println("  -es <url>            Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>        Elasticsearch index name. Default is 'registry'");
        System.out.println("  -shards <number>     Number of shards (partitions) for registry index. Default is 1");
        System.out.println("  -replicas <number>   Number of replicas (extra copies) of registry index. Default is 0");
        System.out.println();
    }

}
