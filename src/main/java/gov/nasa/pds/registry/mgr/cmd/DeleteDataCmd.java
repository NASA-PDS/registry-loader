package gov.nasa.pds.registry.mgr.cmd;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import com.google.gson.Gson;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;


public class DeleteDataCmd implements CliCommand
{
    private String filterMessage;

    public DeleteDataCmd()
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

        String query = buildEsQuery(cmdLine);
        if(query == null)
        {
            throw new Exception("One of the following options is required: -lidvid, -lid, -packageId, -all");
        }

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println(filterMessage);
        System.out.println();
        
        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsClientBuilder.createClient(esUrl);

            // Create request
            Request req = new Request("POST", "/" + indexName + "/_delete_by_query");
            req.setJsonEntity(query);
            
            // Execute request
            Response resp = client.performRequest(req);
            double numDeleted = extractNumDeleted(resp); 
            
            System.out.format("Deleted %.0f document(s)\n", numDeleted);
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

    
    @SuppressWarnings("rawtypes")
    private double extractNumDeleted(Response resp)
    {
        try
        {
            InputStream is = resp.getEntity().getContent();
            Reader rd = new InputStreamReader(is);
            
            Gson gson = new Gson();
            Object obj = gson.fromJson(rd, Object.class);
            rd.close();
            
            obj = ((Map)obj).get("deleted");
            return (Double)obj;
        }
        catch(Exception ex)
        {
            return 0;
        }
    }
    
    
    private String buildEsQuery(CommandLine cmdLine) throws Exception
    {
        EsRequestBuilder bld = new EsRequestBuilder();
        
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            filterMessage = "           LIDVID: " + id;
            return bld.createFilterQuery("lidvid", id);
        }
        
        id = cmdLine.getOptionValue("lid");
        if(id != null)
        {
            filterMessage = "              LID: " + id;
            return bld.createFilterQuery("lid", id);
        }

        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            filterMessage = "       Package ID: " + id;            
            return bld.createFilterQuery("_package_id", id);
        }

        if(cmdLine.hasOption("all"))
        {
            filterMessage = "Delete all documents ";
            return bld.createMatchAllQuery();
        }

        return null;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-data <options>");

        System.out.println();
        System.out.println("Delete data from registry index");
        System.out.println();
        System.out.println("Required parameters, one of:");
        System.out.println("  -lidvid <id>      Delete data by lidvid");
        System.out.println("  -lid <id>         Delete data by lid");
        System.out.println("  -packageId <id>   Delete data by package id"); 
        System.out.println("  -all              Delete all data");
        System.out.println("Optional parameters:");
        System.out.println("  -es <url>         Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
