package gov.nasa.pds.registry.common.es.dao;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.util.CloseUtils;

/**
 * Product data access object. 
 * Provides methods to query Elasticsearch for product information.
 * @author karpenko
 */
public class ProductDao
{
    private Logger log;
    
    private RestClient client;
    private String indexName;

    
    /**
     * Constructor.
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name, e.g., "registry".
     */
    public ProductDao(RestClient client, String indexName)
    {
        log = LogManager.getLogger(this.getClass());
        
        this.client = client;
        this.indexName = indexName;
    }

    
    /**
     * Get product class by LIDVID
     * @param lidvid product LIDVID
     * @return product class, such as "Product_Bundle" or null if the LIDVID doesn't exist.
     * @throws Exception an exception
     */
    public String getProductClass(String lidvid) throws Exception
    {
        if(lidvid == null) return null;
        
        String reqUrl = "/" + indexName + "/_doc/" + lidvid + "?_source=product_class";
        Request req = new Request("GET", reqUrl);
        Response resp = null;
        
        try
        {
            resp = client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            resp = ex.getResponse();
            int code = resp.getStatusLine().getStatusCode();
            // Invalid LIDVID
            if(code == 404 || code == 405) 
            {
                return null;
            }
            else
            {
                throw ex;
            }
        }

        InputStream is = null;
        
        try
        {
            is = resp.getEntity().getContent();
            JsonReader rd = new JsonReader(new InputStreamReader(is));
            
            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("_source".equals(name))
                {
                    return parseProductClassSource(rd);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return null;
    }
    
    
    /**
     * Get number of reference documents (pages) by collection LIDVID and type.
     * @param collectionLidVid collection LIDVID
     * @param type reference type: 'P' - primary, 'S' - secondary
     * @return number of documents (pages)
     * @throws Exception an exception
     */
    public int getRefDocCount(String collectionLidVid, char type) throws Exception
    {
        if(collectionLidVid == null) return 0;
        
        String strType = null;
        if(type == 'P')
        {
            strType = "primary";
        }
        else if(type == 'S')
        {
            strType = "secondary";
        }
        else
        {
            throw new IllegalArgumentException("Invalid type " + type);
        }
        
        // Elasticsearch "Lucene" query
        String query = "collection_lidvid:\"" + collectionLidVid + "\" AND reference_type:" + strType;
        query = URLEncoder.encode(query, "UTF-8");
        
        // Request URL
        String reqUrl = "/" + indexName + "-refs/_count?q=" + query;
        Request req = new Request("GET", reqUrl);
        Response resp = null;

        try
        {
            resp = client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            resp = ex.getResponse();
            int code = resp.getStatusLine().getStatusCode();
            // Invalid LIDVID
            if(code == 404 || code == 405) 
            {
                return 0;
            }
            else
            {
                throw ex;
            }
        }

        InputStream is = null;

        try
        {
            is = resp.getEntity().getContent();
            JsonReader rd = new JsonReader(new InputStreamReader(is));
            
            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("count".equals(name))
                {
                    return rd.nextInt();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return 0;
    }
    
    
    /**
     * Get product references from collection inventory
     * @param collectionLidVid collection LIDVID
     * @param type reference type: 'P' - primary, 'S' - secondary
     * @param page page number starting from 1
     * @return list of product LIDVIDs
     * @throws Exception ResponseException exception - there was a problem calling Elasticsearch,
     * other exceptions - parsing problems (invalid JSON).
     */
    public List<String> getRefs(String collectionLidVid, char type, int page) throws Exception
    {
        if(collectionLidVid == null) return null;
        
        String docId = collectionLidVid + "::" + type + page;
        String reqUrl = "/" + indexName + "-refs/_doc/" + docId + "?_source=product_lidvid";
        Request req = new Request("GET", reqUrl);
        Response resp = null;
        
        try
        {
            resp = client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            resp = ex.getResponse();
            int code = resp.getStatusLine().getStatusCode();
            // Invalid LIDVID
            if(code == 404 || code == 405) 
            {
                return null;
            }
            else
            {
                throw ex;
            }
        }

        InputStream is = null;
        
        try
        {
            is = resp.getEntity().getContent();
            JsonReader rd = new JsonReader(new InputStreamReader(is));
            
            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("_source".equals(name))
                {
                    return parseRefs(rd);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return null;
    }
    
    
    public void updateStatus(List<String> lidvids, String status) throws Exception
    {
        if(lidvids == null || status == null) return;
        
        String json = buildUpdateStatusJson(lidvids, status);
        log.debug("Request:\n" + json);
        
        String reqUrl = "/" + indexName + "/_bulk"; //?refresh=wait_for";
        Request req = new Request("POST", reqUrl);
        req.setJsonEntity(json);
        
        Response resp = client.performRequest(req);
        
        // Check for Elasticsearch errors.
        String respJson = DaoUtils.getLastLine(resp.getEntity().getContent());
        log.debug("Response: " + respJson);
    }
    
    
    public static String buildUpdateStatusJson(List<String> lidvids, String status)
    {
        if(lidvids == null || lidvids.isEmpty()) return null;
        if(status == null || status.isEmpty()) throw new IllegalArgumentException("Status could not be null or empty.");
        
        StringBuilder bld = new StringBuilder();
        String dataLine = "{ \"doc\" : {\"archive_status\" : \"" + status + "\"} }\n";
        
        for(String lidvid: lidvids)
        {
            bld.append("{ \"update\" : {\"_id\" : \"" + lidvid + "\" } }\n");
            bld.append(dataLine);
        }
        
        return bld.toString();
    }
    
    
    private static String parseProductClassSource(JsonReader rd) throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("product_class".equals(name))
            {
                return rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return null;
    }
    

    private static List<String> parseRefs(JsonReader rd) throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("product_lidvid".equals(name))
            {
                return parseList(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return null;
    }

    
    private static List<String> parseList(JsonReader rd) throws Exception
    {
        List<String> list = new ArrayList<>();
        
        rd.beginArray();

        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            list.add(rd.nextString());
        }        
        
        rd.endArray();
        
        return list;
    }


    private void processBulkResponse(String json)
    {
        
    }
}
