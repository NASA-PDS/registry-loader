package gov.nasa.pds.registry.mgr.dao;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;


/**
 * Base abstract class to export data from Elasticsearch. Data is processed in batches. 
 * Elasticsearch "search_after" parameter is used to paginate search results.
 * 
 * @author karpenko
 *
 */
public abstract class DataExporter
{
    private static final int BATCH_SIZE = 100;
    private static final int PRINT_STATUS_SIZE = 5000;
    
    protected Logger log;

    private String esUrl;
    private String indexName;
    private String authConfigFile;
    
   
    /**
     * Constructor
     * @param esUrl Elasticsearch URL, e.g., "app:/connections/direct/localhost.xml"
     * @param indexName Elasticsearch index name
     * @param authConfigFile Elasticsearch authentication configuration file 
     * (see Registry Manager documentation for more info) 
     */
    public DataExporter(String esUrl, String indexName, String authConfigFile)
    {
        this.esUrl = esUrl;
        this.indexName = indexName;
        this.authConfigFile = authConfigFile;

        log = LogManager.getLogger(this.getClass());
    }
    
    
    /**
     * Create JSON query to pass to "/indexName/_search" Elasticsearch API.
     * @param batchSize batch size
     * @param searchAfter Elasticsearch "search_after" parameter to paginate search results.
     * @return JSON 
     * @throws Exception an exception
     */
    protected abstract Request.Search createRequest(Request.Search req, int batchSize, String searchAfter);
    
    
    /**
     * Export data from Elasticsearch into a file
     * @param file a file
     * @throws Exception an exception
     */
    public void export(File file) throws Exception {
      ConnectionFactory conFact = EstablishConnectionFactory.from(this.esUrl, this.authConfigFile)
          .setIndexName(this.indexName);
      try (RestClient client = conFact.createRestClient();
           EsDocWriter writer = new EsDocWriter(file)) {
        String searchAfter = null;
        int numDocs = 0, thisBatchSize;
        do {
          Request.Search req = client.createSearchRequest().setIndex(indexName);
          req = createRequest(req, BATCH_SIZE, searchAfter);
          Response.Search resp = client.performRequest(req);
          thisBatchSize = resp.batch().size();
          numDocs += resp.batch().size();
          searchAfter = ""; // FIXME: needs to be the last ID from the batch()
          if (numDocs % PRINT_STATUS_SIZE == 0) {
            log.info("Exported " + numDocs + " document(s)");
          }
        } while (thisBatchSize == BATCH_SIZE);
        if (numDocs == 0) {
          log.info("No documents found");
        } else {
          log.info("Exported " + numDocs + " document(s)");
        }
        log.info("Done");
      } catch (ResponseException ex) {
        throw new Exception(ex.extractErrorMessage());
      }
    }
  }
