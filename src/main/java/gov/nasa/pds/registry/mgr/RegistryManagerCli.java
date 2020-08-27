package gov.nasa.pds.registry.mgr;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.cmd.CreateRegistryCmd;
import gov.nasa.pds.registry.mgr.cmd.DeleteDataCmd;
import gov.nasa.pds.registry.mgr.cmd.DeleteRegistryCmd;
import gov.nasa.pds.registry.mgr.cmd.ExportDataCmd;
import gov.nasa.pds.registry.mgr.cmd.ExportFileCmd;
import gov.nasa.pds.registry.mgr.cmd.GenerateSchemaCmd;
import gov.nasa.pds.registry.mgr.cmd.LoadDataCmd;
import gov.nasa.pds.registry.mgr.cmd.SetArchiveStatusCmd;
import gov.nasa.pds.registry.mgr.cmd.UpdateSchemaCmd;
import gov.nasa.pds.registry.mgr.util.ExceptionUtils;


public class RegistryManagerCli
{
    private Map<String, CliCommand> commands;
    private CliCommand command;
    private Options options;
    private CommandLine cmdLine;
    
    
    public RegistryManagerCli()
    {
        initCommands();
        initOptions();
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager <command> <options>");

        System.out.println();
        System.out.println("Commands:");
        System.out.println();
        System.out.println("Registry:");
        System.out.println("  create-registry      Create registry index");
        System.out.println("  delete-registry      Delete registry index and all its data");
        System.out.println("  load-data            Load data into registry index");
        System.out.println("  delete-data          Delete data from registry index");
        System.out.println("  export-data          Export data from registry index");
        System.out.println("  export-file          Export a file from blob storage");
        System.out.println("  set-archive-status   Set product archive status");
        
        System.out.println();
        System.out.println("Search:");
        System.out.println("  generate-schema      Generate Elasticsearch schema from one or more PDS data dictionaries");        
        System.out.println("  update-schema        Update Elasticsearch schema from one or more PDS data dictionaries");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -help  Print help for a command");
        
        System.out.println();
        System.out.println("Pass -help after any command to see command-specific usage information, for example,");
        System.out.println("  registry-manager load-data -help");
    }

        
    public void run(String[] args)
    {
        // Print help if there are no command line parameters
        if(args.length == 0)
        {
            printHelp();
            System.exit(1);
        }

        // Parse command line arguments
        if(!parse(args))
        {
            System.out.println();
            printHelp();
            System.exit(1);
        }

        // Run command
        if(!runCommand())
        {
            System.exit(1);
        }        
    }

    
    private boolean runCommand()
    {
        try
        {
            command.run(cmdLine);
        }
        catch(Exception ex)
        {
            System.out.println("[ERROR] " + ExceptionUtils.getMessage(ex));
            return false;
        }
        
        return true;
    }
    
    
    private boolean parse(String[] pArgs)
    {
        try
        {
            CommandLineParser parser = new DefaultParser();
            cmdLine = parser.parse(options, pArgs);
            
            String[] args = cmdLine.getArgs();
            if(args == null || args.length == 0)
            {
                System.out.println("[ERROR] Missing command.");
                return false;
            }

            if(args.length > 1)
            {
                System.out.println("[ERROR] Invalid command: " + String.join(" ", args)); 
                return false;
            }
            
            this.command = commands.get(args[0]);
            if(this.command == null)
            {
                System.out.println("[ERROR] Invalid command: " + args[0]);
                return false;
            }
            
            return true;
        }
        catch(ParseException ex)
        {
            System.out.println("[ERROR] " + ex.getMessage());
            return false;
        }
    }

    
    private void initCommands()
    {
        commands = new HashMap<>();

        // Registry collection create / delete / edit
        commands.put("create-registry", new CreateRegistryCmd());
        commands.put("delete-registry", new DeleteRegistryCmd());

        commands.put("generate-schema", new GenerateSchemaCmd());
        commands.put("update-schema", new UpdateSchemaCmd());

        // Data load / delete / edit
        commands.put("load-data", new LoadDataCmd());
        commands.put("delete-data", new DeleteDataCmd());
        commands.put("export-data", new ExportDataCmd());
        commands.put("export-file", new ExportFileCmd());
        commands.put("set-archive-status", new SetArchiveStatusCmd());
    }
    
    
    private void initOptions()
    {
        options = new Options();
        
        Option.Builder bld;
        
        bld = Option.builder("help");
        options.addOption(bld.build());
        
        bld = Option.builder("es").hasArg().argName("es");
        options.addOption(bld.build());

        bld = Option.builder("file").hasArg().argName("path");
        options.addOption(bld.build());

        bld = Option.builder("configDir").hasArg().argName("dir");
        options.addOption(bld.build());

        bld = Option.builder("config").hasArg().argName("path");
        options.addOption(bld.build());

        bld = Option.builder("outDir").hasArg().argName("dir");
        options.addOption(bld.build());
        
        // delete-data command
        bld = Option.builder("lidvid").hasArg().argName("id");
        options.addOption(bld.build());

        bld = Option.builder("lid").hasArg().argName("id");
        options.addOption(bld.build());

        bld = Option.builder("packageId").hasArg().argName("id");
        options.addOption(bld.build());
        
        bld = Option.builder("all");
        options.addOption(bld.build());
        
        // Status
        bld = Option.builder("status").hasArg().argName("status");
        options.addOption(bld.build());
        
        // Create update collection
        bld = Option.builder("index").hasArg().argName("name");
        options.addOption(bld.build());

        bld = Option.builder("schema").hasArg().argName("path");
        options.addOption(bld.build());

        bld = Option.builder("shards").hasArg().argName("#");
        options.addOption(bld.build());

        bld = Option.builder("replicas").hasArg().argName("#");
        options.addOption(bld.build());
        
        // Logger
        bld = Option.builder("log").hasArg().argName("file");
        options.addOption(bld.build());
        
        bld = Option.builder("v").hasArg().argName("level");
        options.addOption(bld.build());
    }
    
}

