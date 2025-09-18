package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.CloseUtils;
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

        esUrl = CliCommand.getUsersRegistry(cmdLine);
        authPath = cmdLine.getOptionValue("auth");
        
        boolean replace= cmdLine.hasOption("r");

        RestClient client = null;
        
        try
        {
          ConnectionFactory conFact = EstablishConnectionFactory.from(esUrl, authPath);
            client = conFact.createRestClient();
            
            if(replace)
            {
                // Recreate data dictionary index
                IndexService srv = new IndexService(client);
                srv.reCreateIndex("elastic/data-dic.json", conFact.getIndexName() + "-dd");
            }
            
            // Load data
            DataLoader dl = new DataLoader(conFact.setIndexName(conFact.getIndexName() + "-dd"));
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
        System.out.println();
    }

}
