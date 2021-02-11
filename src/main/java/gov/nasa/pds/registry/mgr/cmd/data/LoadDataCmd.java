package gov.nasa.pds.registry.mgr.cmd.data;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.function.BiPredicate;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.DataLoader;
import gov.nasa.pds.registry.mgr.dao.SchemaUpdater;
import gov.nasa.pds.registry.mgr.util.CloseUtils;


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

        String strDir = cmdLine.getOptionValue("dir");
        if(strDir == null) throw new Exception("Missing required parameter '-dir'");
        
        File dir = new File(strDir);
        if(!dir.exists() || !dir.isDirectory()) throw new Exception("Invalid directory " + dir.getAbsolutePath());
        
        String tmp = cmdLine.getOptionValue("updateSchema", "Y");
        boolean updateSchema = parseYesNo("updateSchema", tmp);
        
        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println();

        // Update schema
        if(updateSchema)
        {
            updateSchema(dir);
        }
        
        // Load data
        loadData(dir);
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
    
    
    private void updateSchema(File dir) throws Exception
    {
        File newFields = new File(dir, FIELDS_FILE);
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
    
    
    private void loadData(File dir) throws Exception
    {
        DataLoader registryLoader = new DataLoader(esUrl, indexName, authPath);
        DataLoader refsLoader = new DataLoader(esUrl, indexName + "-refs", authPath);
        refsLoader.setBatchSize(10);

        
        Iterator<Path> it = Files.find(dir.toPath(), 1, new JsonMatcher()).iterator();
        while(it.hasNext())
        {
            File file = it.next().toFile();
            String fileName = file.getName();
            if(fileName.startsWith("registry-docs"))
            {
                registryLoader.loadFile(file);
            }
            else if(fileName.startsWith("refs-docs"))
            {
                refsLoader.loadFile(file);
            }
            else
            {
                System.out.println("[WARN] Unknown file type: " + file.getAbsolutePath());
            }
        }
    }
    

    private static class JsonMatcher implements BiPredicate<Path, BasicFileAttributes>
    {
        @Override
        public boolean test(Path path, BasicFileAttributes attrs)
        {
            String fileName = path.getFileName().toString().toLowerCase();
            return fileName.endsWith(".json");
        }
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-data <options>");

        System.out.println();
        System.out.println("Load data into registry index");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -dir <path>           Harvest output directory to load"); 
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>          Authentication config file");
        System.out.println("  -es <url>             Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>         Elasticsearch index name. Default is 'registry'");
        System.out.println("  -updateSchema <y/n>   Update registry schema. Default is 'yes'");
        System.out.println();
    }

}
