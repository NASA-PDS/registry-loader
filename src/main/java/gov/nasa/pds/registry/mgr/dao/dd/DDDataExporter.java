package gov.nasa.pds.registry.mgr.dao.dd;

import gov.nasa.pds.registry.common.Request.Search;
import gov.nasa.pds.registry.mgr.dao.DataExporter;

/**
 * Exports data dictionary records from Elasticsearch into a file.
 *  
 * @author karpenko
 */
public class DDDataExporter extends DataExporter
{
    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param indexName Elasticsearch index name
     * @param authConfigFile authentication configuration file
     */
    public DDDataExporter(String esUrl, String indexName, String authConfigFile)
    {
        super(esUrl, indexName + "-dd", authConfigFile);
    }
    @Override
    protected Search createRequest(Search req, int batchSize, String searchAfter) {
      return req.all("es_field_name", batchSize, searchAfter);
    }

}
