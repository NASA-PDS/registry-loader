package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.SchemaUpdater;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.Logger;

/**
 * A CLI command to update Elasticsearch schema of the "registry" index.
 * NOTE: This command might be removed in future releases. 
 * The same functionality is implemented by "load-data" command.
 * 
 * @author karpenko
 */
public class UpdateSchemaCmd implements CliCommand
{
    /**
     * Constructor
     */
    public UpdateSchemaCmd()
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

        String dataDir = cmdLine.getOptionValue("dir");
        if(dataDir == null) 
        {
            throw new Exception("Missing required parameter '-dir'");
        }

        RegistryCfg cfg = new RegistryCfg();
        cfg.url = cmdLine.getOptionValue("es", "http://localhost:9200");
        cfg.indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        cfg.authFile = cmdLine.getOptionValue("auth");
        
        Logger.info("Elasticsearch URL: " + cfg.url);
        Logger.info("Index: " + cfg.indexName);

        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient(cfg.url, cfg.authFile);
            SchemaUpdater su = new SchemaUpdater(client, cfg);
            su.updateSchema(new File(dataDir));
            Logger.info("Done");
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

    
    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-schema <options>");

        System.out.println();
        System.out.println("Update Elasticsearch schema");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -dir <path>      Harvest output directory with 'missing_fields.txt' and 'missing_xsds.txt' files");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Authentication config file");
        System.out.println("  -es <url>        Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
