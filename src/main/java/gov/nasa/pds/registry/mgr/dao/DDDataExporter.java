package gov.nasa.pds.registry.mgr.dao;

import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;


public class DDDataExporter extends DataExporter
{
    public DDDataExporter(String esUrl, String indexName, String authConfigFile)
    {
        super(esUrl, indexName + "-dd", authConfigFile);
    }

    
    @Override
    protected String createRequest(int batchSize, String searchAfter) throws Exception
    {
        EsRequestBuilder reqBld = new EsRequestBuilder();
        String json = reqBld.createExportAllDataRequest("es_field_name", batchSize, searchAfter);
        return json;
    }

}
