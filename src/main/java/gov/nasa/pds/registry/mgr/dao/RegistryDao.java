package gov.nasa.pds.registry.mgr.dao;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.common.es.dao.BulkResponseParser;
import gov.nasa.pds.registry.common.util.CloseUtils;
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
    
    
    public void updateAlternateIds(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) return;
        
        RegistryRequestBuilder bld = new RegistryRequestBuilder();
        String json = bld.createUpdateAltIdsRequest(newIds);
        log.debug("Request:\n" + json);
        
        String reqUrl = "/" + indexName + "/_bulk"; //?refresh=wait_for";
        Request req = new Request("POST", reqUrl);
        req.setJsonEntity(json);
        
        Response resp = client.performRequest(req);
        
        // Check for Elasticsearch errors.
        InputStream is = null;
        InputStreamReader rd = null;
        try
        {
            is = resp.getEntity().getContent();
            rd = new InputStreamReader(is);
            
            BulkResponseParser parser = new BulkResponseParser();
            parser.parse(rd);
        }
        finally
        {
            CloseUtils.close(rd);
            CloseUtils.close(is);
        }
    }

}
