package gov.nasa.pds.registry.mgr.dao;

import java.io.File;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsDocWriter;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;
import gov.nasa.pds.registry.mgr.util.es.SearchResponseParser;


public abstract class DataExporter
{
    private static final int BATCH_SIZE = 100;
    private static final int PRINT_STATUS_SIZE = 5000;
    
    private String esUrl;
    private String indexName;
    private String authConfigFile;
    
    
    public DataExporter(String esUrl, String indexName, String authConfigFile)
    {
        this.esUrl = esUrl;
        this.indexName = indexName;
        this.authConfigFile = authConfigFile;
    }
    
    
    protected abstract String createRequest(int batchSize, String searchAfter) throws Exception;
    
    
    public void export(File file) throws Exception
    {
        EsDocWriter writer = null; 
        RestClient client = null;
        
        try
        {
            writer = new EsDocWriter(file);
            client = EsClientFactory.createRestClient(esUrl, authConfigFile);
            SearchResponseParser parser = new SearchResponseParser();
            
            String searchAfter = null;
            int numDocs = 0;
            
            do
            {
                Request req = new Request("GET", "/" + indexName + "/_search");
                String json = createRequest(BATCH_SIZE, searchAfter);
                req.setJsonEntity(json);
                
                Response resp = client.performRequest(req);
                parser.parseResponse(resp, writer);
                
                numDocs += parser.getNumDocs();
                searchAfter = parser.getLastId();
                
                if(numDocs % PRINT_STATUS_SIZE == 0)
                {
                    System.out.println("Exported " + numDocs + " document(s)");
                }
            }
            while(parser.getNumDocs() == BATCH_SIZE);

            if(numDocs == 0)
            {
                System.out.println("No documents found");
            }
            else
            {
                System.out.println("Exported " + numDocs + " document(s)");
            }
            
            System.out.println("Done");
        }
        catch(ResponseException ex)
        {
            throw new Exception(EsUtils.extractErrorMessage(ex));
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(writer);
        }

    }
}
