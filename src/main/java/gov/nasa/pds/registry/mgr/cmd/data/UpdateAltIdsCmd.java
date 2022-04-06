package gov.nasa.pds.registry.mgr.cmd.data;


import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to update product's alternate IDs in Elasticsearch.
 * 
 * @author karpenko
 */
public class UpdateAltIdsCmd implements CliCommand
{

    
    /**
     * Constructor
     */
    public UpdateAltIdsCmd()
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

        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-alt-ids <options>");

        System.out.println();
        System.out.println("Update product's alternate IDs");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>      CSV file with the list of IDs");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
