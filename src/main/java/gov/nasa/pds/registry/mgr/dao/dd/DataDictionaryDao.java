package gov.nasa.pds.registry.mgr.dao.dd;

import java.util.Map;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.mgr.dao.SchemaRequestBuilder;


/**
 * Data dictionary DAO (Data Access Object).
 * This class provides methods to read and update data dictionary. 
 * @author karpenko
 */
public class DataDictionaryDao
{
    private RestClient client;
    private String indexName;

    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     */
    public DataDictionaryDao(RestClient client, String indexName)
    {
        this.client = client;
        this.indexName = indexName;
    }

    
    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class GetLddInfoRespParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public LddInfo info;
        
        public GetLddInfoRespParser()
        {
            info = new LddInfo();
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                String strDate = (String)map.get("date");
                info.updateDate(strDate);
                
                String file = (String)map.get("attr_name");
                info.addSchemaFile(file);
            }
        }
    }
    

    /**
     * Get LDD date from data dictionary index in Elasticsearch.
     * @param indexName Elasticsearch base index name, e.g., "registry". 
     * NOTE: don't use full index name, like "registry-dd". 
     * @param namespace LDD namespace, e.g., "pds", "geom", etc.
     * @return ISO instant class representing LDD date.
     * @throws Exception an exception
     */
    public LddInfo getLddInfo(String namespace) throws Exception
    {
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createGetLddInfoRequest(namespace);

        Request req = new Request("GET", "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
        
        GetLddInfoRespParser parser = new GetLddInfoRespParser();
        parser.parseResponse(resp, parser); 
        return parser.info;
    }

}
