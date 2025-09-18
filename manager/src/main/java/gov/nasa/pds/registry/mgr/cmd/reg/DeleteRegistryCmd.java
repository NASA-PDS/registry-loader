package gov.nasa.pds.registry.mgr.cmd.reg;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.util.CloseUtils;
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
        
        String esUrl = CliCommand.getUsersRegistry(cmdLine);
        String authPath = cmdLine.getOptionValue("auth");

        RestClient client = null;
        
        try
        {
          ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
            client = conFact.createRestClient();
            IndexService srv = new IndexService(client);
            String indexName = conFact.getIndexName();

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
        System.out.println("  -es <url>         (deprecated) File URI to the configuration to connect to the registry. For example, file:///home/user/.pds/mcp.xml. Default is app:/connections/direct/localhost.xml");
        System.out.println("  -registry <url>   File URI to the configuration to connect to the registry. For example, file:///home/user/.pds/mcp.xml. Default is app:/connections/direct/localhost.xml");
    }

}
