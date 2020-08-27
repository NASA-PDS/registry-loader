package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class DeleteRegistryCmd implements CliCommand
{
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

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println();

        RestClient client = null;
        
        try
        {
            System.out.println("Deleting index...");

            // Create Elasticsearch client
            client = EsClientBuilder.createClient(esUrl);
            
            // Create request
            Request req = new Request("DELETE", "/" + indexName);

            // Execute request
            Response resp = client.performRequest(req);
            EsUtils.printWarnings(resp);
            System.out.println("Done");
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
        System.out.println("Usage: registry-manager delete-registry <options>");

        System.out.println();
        System.out.println("Delete registry index and all its data");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
    }

}
