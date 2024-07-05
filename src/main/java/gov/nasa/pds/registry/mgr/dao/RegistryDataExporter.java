package gov.nasa.pds.registry.mgr.dao;

import gov.nasa.pds.registry.common.Request.Search;

/**
 * Exports data records from Elasticsearch "registry" index into a file.
 * 
 * @author karpenko
 */
public class RegistryDataExporter extends DataExporter
{
    private String filterFieldName;
    private String filterFieldValue;

    
    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param indexName Elasticsearch index name
     * @param authConfigFile authentication configuration file
     */
    public RegistryDataExporter(String esUrl, String indexName, String authConfigFile)
    {
        super(esUrl, indexName, authConfigFile);
    }
    

    /**
     * Filter data by LIDVID, LID, PackageId, etc. 
     * If a filter is not set, all data will be exported.
     * @param name Elasticsearch field name
     * @param value field value
     */
    public void setFilterField(String name, String value)
    {
        this.filterFieldName = name;
        this.filterFieldValue = value;
    }
    
   @Override
    protected Search createRequest(Search req, int batchSize, String searchAfter) {
      return filterFieldName == null ? req.all("lidvid", batchSize, searchAfter) :
        req.all(this.filterFieldName, this.filterFieldValue, "lidvid", batchSize, searchAfter);
    }

}
