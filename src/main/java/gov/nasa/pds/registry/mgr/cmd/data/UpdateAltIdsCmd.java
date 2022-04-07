package gov.nasa.pds.registry.mgr.cmd.data;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;


/**
 * A CLI command to update product's alternate IDs in Elasticsearch.
 * 
 * @author karpenko
 */
public class UpdateAltIdsCmd implements CliCommand
{
    
    /**
     * Constructor
     */
    public UpdateAltIdsCmd()
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

        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);
        String authPath = cmdLine.getOptionValue("auth");

        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) throw new Exception("Missing required parameter '-file'");
        File file = new File(filePath);
        if(!file.exists()) throw new Exception("Input file doesn't exist: " + file.getAbsolutePath());
        
        RestClient client = null;
        
        try
        {
            client = EsClientFactory.createRestClient(esUrl, authPath);

            updateIds(client, file);
            
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

    
    private void updateIds(RestClient client, File file) throws Exception
    {
        BufferedReader rd = null;
        Logger log = LogManager.getLogger(this.getClass());

        try
        {
            rd = new BufferedReader(new FileReader(file));
            
            String line;
            int lineNum = 0;
            
            while((line = rd.readLine()) != null)
            {
                lineNum++;
                line = line.trim();
                
                String[] ids = StringUtils.split(line, ",;\t ");
                if(ids == null || ids.length != 2)
                {
                    log.warn("Line " + lineNum + " has invalid value: [" + line + "]");
                    continue;
                }
                
                if(!ids[0].contains("::"))
                {
                    log.warn("Line " + lineNum + " has invalid LIDVID: [" + ids[0] + "]");
                    continue;
                }
                
                if(!ids[1].contains("::"))
                {
                    log.warn("Line " + lineNum + " has invalid LIDVID: [" + ids[1] + "]");
                    continue;
                }

                updateIds(client, ids);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void updateIds(RestClient client, String[] lidvids) throws Exception
    {
        System.out.println(lidvids[0]);
        System.out.println(lidvids[1]);
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-alt-ids <options>");

        System.out.println();
        System.out.println("Update product's alternate IDs");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>      CSV file with the list of IDs");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
