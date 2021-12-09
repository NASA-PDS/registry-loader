package gov.nasa.pds.registry.mgr.dao.dd;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;


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
        public LddVersions info;
        
        public GetLddInfoRespParser()
        {
            info = new LddVersions();
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
    public LddVersions getLddInfo(String namespace) throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder();
        String json = bld.createListLddsRequest(namespace);

        Request req = new Request("GET", "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
        
        GetLddInfoRespParser parser = new GetLddInfoRespParser();
        parser.parseResponse(resp, parser); 
        return parser.info;
    }


    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class ListLddsParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public List<LddInfo> list;
        
        public ListLddsParser()
        {
            list = new ArrayList<>();
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                LddInfo info = new LddInfo();
                
                // Namespace
                info.namespace = (String)map.get("attr_ns");
                
                // Date
                String str = (String)map.get("date");
                if(str != null && !str.isEmpty())
                {
                    info.date = Instant.parse(str);
                }
                
                // Versions
                info.imVersion = (String)map.get("im_version");
                
                // File name
                info.file = (String)map.get("attr_name");
                
                list.add(info);                
            }
        }
    }

    
    /**
     * List registered LDDs
     * @param namespace if this parameter is null list all LDDs
     * @return a list of LDDs
     * @throws Exception an exception
     */
    public List<LddInfo> listLdds(String namespace) throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder();
        String json = bld.createListLddsRequest(namespace);

        Request req = new Request("GET", "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);

        ListLddsParser parser = new ListLddsParser();
        parser.parseResponse(resp, parser); 
        return parser.list;
    }
}
