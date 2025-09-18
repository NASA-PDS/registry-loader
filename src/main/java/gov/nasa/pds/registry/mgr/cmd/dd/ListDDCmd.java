package gov.nasa.pds.registry.mgr.cmd.dd;

import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddInfo;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;

/**
 * A CLI command to list data dictionary registered in Elasticsearch.
 *  
 * @author karpenko
 */
public class ListDDCmd implements CliCommand
{
    /**
     * Constructor
     */
    public ListDDCmd()
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

        String authFile = cmdLine.getOptionValue("auth");
        String namespace = cmdLine.getOptionValue("ns");
        String url = CliCommand.getUsersRegistry(cmdLine);

        if (authFile == null || authFile.isBlank()) {
          throw new IllegalArgumentException("missing argument: -auth must be given");
        }
        if (namespace == null || namespace.isBlank()) {
          throw new IllegalArgumentException("missing argument: -ns must be given");         
        }
        try
        {
            RegistryManager.init(url, authFile);
            
            DataDictionaryDao dao = RegistryManager.getInstance().getDataDictionaryDao();
            List<LddInfo> list = dao.listLdds(namespace);
            Collections.sort(list);
            
            System.out.println();
            System.out.format("%-20s %-40s %10s   %s\n", "Namespace", "File", "Version", "Date");
            System.out.println("-----------------------------------------------------------------------------------------------");
            
            for(LddInfo info: list)
            {
                System.out.format("%-20s %-40s %10s   %s\n", info.namespace, info.file, info.imVersion, info.date);
            }
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
        System.out.println("Usage: registry-manager list-dd <options>");

        System.out.println();
        System.out.println("List data dictionaries");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -ns <namespace>   LDD namespace. Can be used with -dd parameter.");        
        System.out.println();
    }
    
}
