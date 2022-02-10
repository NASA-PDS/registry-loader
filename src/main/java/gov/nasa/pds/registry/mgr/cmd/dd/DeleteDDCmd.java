package gov.nasa.pds.registry.mgr.cmd.dd;

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

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.RegistryRequestBuilder;


/**
 * A CLI command to delete records from the data dictionary index in Elasticsearch.
 * Data can be deleted by ID, or namespace. All data can be also deleted.
 *  
 * @author karpenko
 */
public class DeleteDDCmd implements CliCommand
{
    private String esUrl;
    private String indexName;
    private String authPath;

    /**
     * Constructor
     */
    public DeleteDDCmd()
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
        
        String id = cmdLine.getOptionValue("id");
        if(id != null)
        {
            deleteById(id);
            return;
        }
        
        String ns = cmdLine.getOptionValue("ns");
        if(ns != null)
        {
            deleteByNamespace(ns);
            return;
        }

        throw new Exception("One of the following options is required: -id, -ns");
    }

    
    private void deleteById(String id) throws Exception
    {
        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsClientFactory.createRestClient(esUrl, authPath);

            // Create request
            RegistryRequestBuilder bld = new RegistryRequestBuilder();
            String query = bld.createFilterQuery("_id", id);
            
            Request req = new Request("POST", "/" + indexName + "-dd" + "/_delete_by_query?refresh=true");
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

    
    private void deleteByNamespace(String ns) throws Exception
    {
        RestClient client = null;
        
        try
        {
            // Create Elasticsearch client
            client = EsClientFactory.createRestClient(esUrl, authPath);

            // (1) Delete by class namespace
            
            // Create request
            RegistryRequestBuilder bld = new RegistryRequestBuilder();
            String query = bld.createFilterQuery("class_ns", ns);
            
            Request req = new Request("POST", "/" + indexName + "-dd" + "/_delete_by_query?refresh=true");
            req.setJsonEntity(query);
            
            // Execute request
            Response resp = client.performRequest(req);
            double numDeleted = extractNumDeleted(resp); 
            
            // (2) Delete by attribute namespace
            
            // Create request
            query = bld.createFilterQuery("attr_ns", ns);
            
            req = new Request("POST", "/" + indexName + "-dd" + "/_delete_by_query?refresh=true");
            req.setJsonEntity(query);
            
            // Execute request
            resp = client.performRequest(req);
            numDeleted += extractNumDeleted(resp); 
            
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
    
    
    /**
     * Extract number of deleted records from Elasticsearch delete API response.
     * @param resp
     * @return number of deleted records
     */
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
    
    
    /**
     * Print help screen
     */
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-dd <options>");

        System.out.println();
        System.out.println("Delete data from data dictionary index");
        System.out.println();
        System.out.println("Required parameters, one of:");
        System.out.println("  -id <id>          Delete data by ID (Full field name)");
        System.out.println("  -ns <namespace>   Delete data by namespace");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>     Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
