package gov.nasa.pds.registry.mgr.cmd.dd;

import java.io.File;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.dd.DDDataExporter;


/**
 * A CLI command to export data dictionary index ("registry-dd") from Elasticsearch. 
 * 
 * @author karpenko
 */
public class ExportDDCmd implements CliCommand
{
    /**
     * Constructor
     */
    public ExportDDCmd()
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
        
        String esUrl = CliCommand.getUsersRegistry(cmdLine);
        String authPath = cmdLine.getOptionValue("auth");

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println();
        
        DDDataExporter exp = new DDDataExporter(esUrl, authPath);
        exp.export(new File(filePath));
    }

    
    /**
     * Print help screen.
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-dd <options>");

        System.out.println();
        System.out.println("Export data from registry data dictionary");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>      Output file path");        
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         (deprecated) File URI to the configuration to connect to the registry. For example, file:///home/user/.pds/mcp.xml. Default is app:/connections/direct/localhost.xml");
        System.out.println("  -registry <url>   File URI to the configuration to connect to the registry. For example, file:///home/user/.pds/mcp.xml. Default is app:/connections/direct/localhost.xml");
        System.out.println();
    }

    
}
