package gov.nasa.pds.registry.mgr.cmd.data;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dao.SchemaUpdater;
import gov.nasa.pds.registry.mgr.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class LoadDataCmd implements CliCommand
{
    private static final String FIELDS_FILE = "fields.txt";
    
    private String esUrl;
    private String indexName;
    private String authPath;
    
    
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

        esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        authPath = cmdLine.getOptionValue("auth");

        // Get list of files to load
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }
        
        String tmp = cmdLine.getOptionValue("updateSchema", "Y");
        boolean updateSchema = parseYesNo("updateSchema", tmp);
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println();

        // Update schema
        if(updateSchema)
        {
            updateSchema(filePath);
        }
        
        // Load data
        loadData(filePath);
    }

    
    private boolean parseYesNo(String paramName, String val) throws Exception
    {
        val = val.toLowerCase();
        
        if(val.equals("y") || val.equals("yes"))
        {
            return true;
        }
        
        if(val.equals("n") || val.equals("no"))
        {
            return false;
        }
        
        throw new Exception("Parameter '" + paramName + "' has invalid value '" + val + "'");
    }
    
    
    private void updateSchema(String filePath) throws Exception
    {
        File newFields = getFieldListFile(filePath);
        System.out.println("Updating schema with fields from " + newFields.getAbsolutePath());
        
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);
            SchemaUpdater su = new SchemaUpdater(client, indexName);
            su.updateSchema(newFields);
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
    
    
    private File getFieldListFile(String filePath)
    {
        File file = new File(filePath);
        if(file.isDirectory())
        {
            return new File(file, FIELDS_FILE);
        }
        
        return new File(file.getParent(), FIELDS_FILE);
    }
    
    
    private void loadData(String filePath) throws Exception
    {
        System.out.println("Loading data...");
        
        List<File> files = getFiles(filePath);
        if(files == null || files.isEmpty()) return;

        DataLoader loader = new DataLoader(esUrl, indexName, authPath);
        
        for(File file: files)
        {
            loader.loadFile(file);
        }
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
        System.out.println("  -file <path>          A JSON file (generated by Harvest) or a directory to load."); 
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>          Authentication config file");
        System.out.println("  -es <url>             Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>         Elasticsearch index name. Default is 'registry'");
        System.out.println("  -updateSchema <y/n>   Update registry schema. Default is 'yes'");
        System.out.println();
    }

}
