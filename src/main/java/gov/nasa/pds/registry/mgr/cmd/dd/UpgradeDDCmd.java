package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.srv.IndexService;


/**
 * A CLI command to upgrade data dictionary index in Elasticsearch.
 *  
 * @author karpenko
 */
public class UpgradeDDCmd implements CliCommand
{
    private String esUrl;
    private String indexName;
    private String authPath;

    /**
     * Constructor
     */
    public UpgradeDDCmd()
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

        esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        authPath = cmdLine.getOptionValue("auth");
        
        boolean replace= cmdLine.hasOption("r");

        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);
            
            if(replace)
            {
                // Recreate data dictionary index
                IndexService srv = new IndexService(client);
                srv.reCreateIndex("elastic/data-dic.json", indexName + "-dd");
            }
            
            // Load data
            DataLoader dl = new DataLoader(esUrl, indexName + "-dd", authPath);
            File zipFile = IndexService.getDataDicFile();
            dl.loadZippedFile(zipFile, "dd.json");
        }
        finally
        {
            CloseUtils.close(client);
        }

    }

    
    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager upgrade-dd <options>");

        System.out.println();
        System.out.println("Upgrade data dictionary index");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -r              Recreate data dictionary index (replace old data dictionary)");
        System.out.println("  -auth <file>    Authentication config file");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
