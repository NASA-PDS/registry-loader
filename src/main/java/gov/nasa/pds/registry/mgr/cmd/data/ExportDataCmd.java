package gov.nasa.pds.registry.mgr.cmd.data;

import java.io.File;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.RegistryDataExporter;


public class ExportDataCmd implements CliCommand
{
    private String filterFieldName;
    private String filterFieldValue;
    
    
    public ExportDataCmd()
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
        
        // File path
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }
        
        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");

        String msg = extractFilterParams(cmdLine);
        if(msg == null)
        {
            throw new Exception("One of the following options is required: -lidvid, -packageId, -all");
        }

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println(msg);
        System.out.println();
        
        RegistryDataExporter exp = new RegistryDataExporter(esUrl, indexName, authPath);
        exp.setFilterField(filterFieldName, filterFieldValue);
        exp.export(new File(filePath));
    }

    
    private String extractFilterParams(CommandLine cmdLine) throws Exception
    {
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            filterFieldName = "lidvid";
            filterFieldValue = id;
            return "           LIDVID: " + id;
        }
        
        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            filterFieldName = "_package_id";
            filterFieldValue = id;
            return "       Package ID: " + id;            
        }

        if(cmdLine.hasOption("all"))
        {
            return "Export all documents ";
        }

        return null;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-data <options>");

        System.out.println();
        System.out.println("Export data from registry index");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>      Output file path");        
        System.out.println("  -lidvid <id>      Export data by lidvid");
        System.out.println("  -packageId <id>   Export data by package id");
        System.out.println("  -all              Export all data");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

    
}
