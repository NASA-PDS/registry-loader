package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.es.DataLoader;


public class LoadDataCmd implements CliCommand
{
    public LoadDataCmd()
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

        String esUrl = cmdLine.getOptionValue("url", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);

        // Get list of files to load
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }
        
        List<File> files = getFiles(filePath);
        if(files == null || files.isEmpty()) return;

        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println();

        DataLoader loader = new DataLoader(esUrl, indexName);
        
        for(File file: files)
        {
            loader.loadFile(file);
        }
        
        System.out.println("Done");
    }

    
    private static class JsonFileFilter implements FileFilter
    {
        public JsonFileFilter()
        {
        }
        
        @Override
        public boolean accept(File file)
        {
            if(!file.isFile()) return false;

            if(file.getName().toLowerCase().endsWith(".json")) return true;
            
            return false;
        }
    }

    
    private List<File> getFiles(String filePath) throws Exception
    {
        File file = new File(filePath);

        if(file.isDirectory())
        {
            File[] ff = file.listFiles(new JsonFileFilter());
            if(ff == null || ff.length == 0)
            {
                System.out.println("Could not find any JSON files in " + file.getAbsolutePath());
                return null;
            }
            
            return Arrays.asList(ff);
        }
        else
        {
            // Check if the file exists
            if(!file.exists()) throw new Exception("File does not exist: " + file.getAbsolutePath());
        
            List<File> list = new ArrayList<>(1);
            list.add(file);
            return list;
        }
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-data <options>");

        System.out.println();
        System.out.println("Load data into registry index");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>    A JSON file or a directory to load."); 
        System.out.println("Optional parameters:");
        System.out.println("  -url <url>      Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
