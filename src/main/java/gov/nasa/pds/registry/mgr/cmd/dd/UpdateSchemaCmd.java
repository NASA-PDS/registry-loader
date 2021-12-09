package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;

import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.schema.SchemaUpdater;


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

        boolean fixMissingFDs = cmdLine.hasOption("fixMissingFD");
        
        RegistryManager.init(cfg);
        
        try
        {
            SchemaUpdater su = new SchemaUpdater(cfg, fixMissingFDs);
            su.updateLddsAndSchema(new File(dataDir));
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
        finally
        {
            RegistryManager.destroy();
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
        System.out.println("  -fixMissingFD    Use 'keyword' ES datatype for missing field definitions.");        
        System.out.println();
    }

}
