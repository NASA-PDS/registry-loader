package gov.nasa.pds.registry.mgr.cmd.reg;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.srv.IndexService;


/**
 * A CLI command to delete registry indices (registry, registry-dd, registry-refs)
 * from Elasticsearch.
 *  
 * @author karpenko
 */
public class DeleteRegistryCmd implements CliCommand
{
    /**
     * Constructor
     */
    public DeleteRegistryCmd()
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

        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);
            IndexService srv = new IndexService(client);

            srv.deleteIndex(indexName);
            srv.deleteIndex(indexName + "-refs");
            srv.deleteIndex(indexName + "-dd");
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-registry <options>");

        System.out.println();
        System.out.println("Delete registry index and all its data");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>    Authentication config file");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
    }

}
