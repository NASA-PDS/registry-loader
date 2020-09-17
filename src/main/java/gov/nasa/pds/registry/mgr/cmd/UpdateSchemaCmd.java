package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.schema.SchemaUpdater;
import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class UpdateSchemaCmd implements CliCommand
{
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

        String cfgPath = cmdLine.getOptionValue("config");
        if(cfgPath == null) 
        {
            throw new Exception("Missing required parameter '-config'");
        }

        // Read configuration file
        File cfgFile = new File(cfgPath);
        System.out.println("Reading configuration from " + cfgFile.getAbsolutePath());
        ConfigReader cfgReader = new ConfigReader();
        Configuration cfg = cfgReader.read(cfgFile);
        
        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");
        
        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsUtils.createClient(esUrl, authPath);

            // Update Elasticsearch schema
            updateSchema(cfg, client, indexName);
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
        System.out.println("Usage: registry-manager update-schema <options>");

        System.out.println();
        System.out.println("Update Elasticsearch schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -config <path>   Configuration file.");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>     Authentication config file");
        System.out.println("  -es <url>        Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>    Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

    
    private void updateSchema(Configuration cfg, RestClient client, String indexName) throws Exception
    {
        SchemaUpdater upd = new SchemaUpdater(cfg, client, indexName);
        
        for(File file: cfg.dataDicFiles)
        {
            System.out.println("Processing data dictionary " + file.getAbsolutePath());
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            upd.updateSchema(dd);
        }
    }
}
