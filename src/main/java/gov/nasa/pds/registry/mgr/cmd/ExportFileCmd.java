package gov.nasa.pds.registry.mgr.cmd;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.EmbeddedBlobExporter;
import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;
import gov.nasa.pds.registry.mgr.util.es.SearchResponseParser;


public class ExportFileCmd implements CliCommand
{
    private static class ResponseCB implements SearchResponseParser.Callback
    {
        private boolean found = false; 
        private String lidvid;
        private String filePath;
        
        
        public ResponseCB(String lidvid, String filePath)
        {
            this.lidvid = lidvid;
            this.filePath = filePath;
        }
        
        
        @Override
        @SuppressWarnings("rawtypes")
        public void onRecord(String id, Object rec) throws Exception
        {
            found = true;
         
            Object blob = ((Map)rec).get("_file_blob");
            if(blob == null)
            {
                System.out.println("There is no BLOB in a document with LIDVID = " + lidvid);
                System.out.println("Probably embedded BLOB storage was not enabled when the document was created.");
                return;
            }

            EmbeddedBlobExporter.export(blob.toString(), filePath);
            System.out.println("Done");
        }
        
        
        public boolean found()
        {
            return found;
        }
        
    }
    
    
    public ExportFileCmd()
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
        
        // Lidvid
        String lidvid = cmdLine.getOptionValue("lidvid");
        if(lidvid == null) 
        {
            throw new Exception("Missing required parameter '-lidvid'");
        }
        
        // File path
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println("           LIDVID: " + lidvid);
        System.out.println("      Output file: " + filePath);
        System.out.println();

        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsClientBuilder.createClient(esUrl);

            // Create request
            Request req = new Request("GET", "/" + indexName + "/_search");
            EsRequestBuilder bld = new EsRequestBuilder();
            String jsonReq = bld.createGetBlobRequest(lidvid);
            req.setJsonEntity(jsonReq);
            
            // Execute request
            Response resp = client.performRequest(req);

            SearchResponseParser respParser = new SearchResponseParser();
            ResponseCB cb = new ResponseCB(lidvid, filePath);
            respParser.parseResponse(resp, cb);
            
            if(!cb.found())
            {
                System.out.println("Could not find a document with lidvid = " + lidvid);
                return;
            }
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
        System.out.println("Usage: registry-manager export-file <options>");

        System.out.println();
        System.out.println("Export a file from blob storage");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -lidvid <id>    Lidvid of a file to export from blob storage");
        System.out.println("  -file <path>    Output file path");
        System.out.println("Optional parameters:");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
