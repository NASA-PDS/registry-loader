package gov.nasa.pds.registry.mgr.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.mgr.dao.resp.GetAltIdsParser;


public class RegistryDao
{
    private Logger log;
    private RestClient client;
    private String indexName;

    private boolean pretty = false;

    
    public RegistryDao(RestClient client, String indexName)
    {
        log = LogManager.getLogger(this.getClass());
        this.client = client;
        this.indexName = indexName;
    }

    
    public void setPretty(boolean b)
    {
        this.pretty = b;
    }
    
    
    public Map<String, Set<String>> getAlternateIds(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;
        
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String jsonReq = bld.createGetAlternateIdsRequest(ids);
        
        String reqUrl = "/" + indexName + "/_search";
        if(pretty) reqUrl += "?pretty";
        
        Request req = new Request("GET", reqUrl);
        req.setJsonEntity(jsonReq);
        Response resp = client.performRequest(req);

        //DebugUtils.dumpResponseBody(resp);
        
        GetAltIdsParser cb = new GetAltIdsParser();
        SearchResponseParser parser = new SearchResponseParser();
        parser.parseResponse(resp, cb);
        
        return cb.getIdMap();
    }
}
