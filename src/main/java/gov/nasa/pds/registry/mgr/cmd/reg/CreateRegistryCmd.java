package gov.nasa.pds.registry.mgr.cmd.reg;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.srv.IndexService;


/**
 * A CLI command to create registry indices (registry, registry-dd, registry-refs)
 * in Elasticsearch. Also, default data dictionary data is loaded into "registry-dd" index.
 * 
 * @author karpenko
 */
public class CreateRegistryCmd implements CliCommand
{
    
    /**
     * Constructor
     */
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

        String esUrl = cmdLine.getOptionValue("es", "app://connections/direct/localhost.xml");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");
        
        int shards = parseShards(cmdLine.getOptionValue("shards", "1"));
        int replicas = parseReplicas(cmdLine.getOptionValue("replicas", "0"));
        
        RestClient client = null;

        try
        {
          ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
            client = conFact.createRestClient();
            IndexService srv = new IndexService(client);
            
            // Registry
            srv.createIndex("elastic/registry.json", indexName, shards, replicas);

            // Collection inventory (product references)
            srv.createIndex("elastic/refs.json", indexName + "-refs", shards, replicas);
            
            // Data dictionary
            srv.createIndex("elastic/data-dic.json", indexName + "-dd", 1, replicas);
            // Load data
            DataLoader dl = new DataLoader(conFact.clone().setIndexName(indexName + "-dd"));
            File zipFile = IndexService.getDataDicFile();
            dl.loadZippedFile(zipFile, "dd.json");
        }
        finally
        {
            CloseUtils.close(client);
        }
    }


    /**
     * Parse and validate "-shards" parameter
     * @param str
     * @return
     * @throws Exception
     */
    private int parseShards(String str) throws Exception
    {
        int val = parseInt(str);
        if(val <= 0) throw new Exception("Invalid number of shards: " + str);
        
        return val;
    }
    

    /**
     * Parse and validate "-replicas" parameter
     * @param str
     * @return
     * @throws Exception
     */
    private int parseReplicas(String str) throws Exception
    {
        int val = parseInt(str);
        if(val < 0) throw new Exception("Invalid number of replicas: " + str);
        
        return val;
    }

    
    /**
     * Parse integer
     * @param str a string to convert to int
     * @return parsed int
     */
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
    
    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry index");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>         Authentication config file");
        System.out.println("  -es <url>            Elasticsearch URL. Default is app:/connections/direct/localhost.xml");
        System.out.println("  -index <name>        Elasticsearch index name. Default is 'registry'");
        System.out.println("  -shards <number>     Number of shards (partitions) for registry index. Default is 1");
        System.out.println("  -replicas <number>   Number of replicas (extra copies) of registry index. Default is 0");
        System.out.println();
    }

}
