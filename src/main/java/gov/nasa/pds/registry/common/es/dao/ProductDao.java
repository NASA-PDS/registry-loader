package gov.nasa.pds.registry.common.es.dao;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.common.util.LidVidUtils;

/**
 * Product data access object. 
 * Provides methods to query Elasticsearch for product information.
 * @author karpenko
 */
public class ProductDao
{    
    private RestClient client;
    private String indexName;
        
    /**
     * Constructor.
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name, e.g., "registry".
     */
    public ProductDao(RestClient client, String indexName)
    {
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
        
        Request.Get req = client.createGetRequest()
            .includeField("product_class")
            .setId(lidvid)
            .setIndex(this.indexName);
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
        Request.Count req = client.createCountRequest()
            .setIndex(this.indexName + "-refs")
            .setQuery(query);
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
        Request.Get req = client.createGetRequest()
            .includeField("product_lidvid")
            .setId(docId)
            .setIndex(this.indexName + "-refs");
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
    
    
    /**
     * Update archive status by lidvids
     * @param lidvids a list of lidvids to update
     * @param status new status
     * @throws Exception an exception
     */
    public void updateArchiveStatus(Collection<String> lidvids, String status) throws Exception
    {
        if(lidvids == null || status == null) return;
        
        Request.Bulk req = client.createBulkRequest()
            .buildUpdateStatus(lidvids, status)
            .setIndex(this.indexName);
        
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
                return DaoUtils.parseList(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return null;
    }

    
    /**
     * Get collection references of a bundle. References can be either LIDs, LIDVIDs or both.
     * @param bundleLidvid bundle LIDVID
     * @return Collection references
     * @throws Exception an exception
     */
    public LidvidSet getCollectionIds(String bundleLidvid) throws Exception
    {
        if(bundleLidvid == null) return null;
        
        Request.Get req = client.createGetRequest()
            .includeField("ref_lidvid_collection")
            .includeField("ref_lid_collection")
            .setId(bundleLidvid)
            .setIndex(this.indexName);
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

        LidvidSet collectionIds = null;
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
                    collectionIds = parseCollectionIdsSource(rd);
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
        
        if(collectionIds == null || collectionIds.lidvids == null 
                || collectionIds.lids == null) return collectionIds;
        
        // Harvest converts LIDVIDs to LIDs, so let's delete those converted LIDs.
        for(String lidvid: collectionIds.lidvids)
        {
            String lid = LidVidUtils.lidvidToLid(lidvid);
            if(lid != null)
            {
                collectionIds.lids.remove(lid);
            }
        }
        
        
        return collectionIds;
    }

    
    private static LidvidSet parseCollectionIdsSource(JsonReader rd) throws Exception
    {
        LidvidSet ids = new LidvidSet();
        
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("ref_lid_collection".equals(name))
            {
                ids.lids = DaoUtils.parseSet(rd);
            }
            else if("ref_lidvid_collection".equals(name))
            {
                ids.lidvids = DaoUtils.parseSet(rd);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return ids;
    }

    
    /**
     * Given a list of LIDs, find latest versions
     * @param lids a list of LIDs
     * @return a list of latest LIDVIDs
     * @throws Exception an exception
     */
    public List<String> getLatestLidVids(Collection<String> lids) throws Exception
    {
        if(lids == null || lids.isEmpty()) return null;
        
        Request.Search req = client.createSearchRequest()
            .setIndex(this.indexName)
            .buildLatestLidVids(lids);        
        Response resp = null;
        
        try
        {
            resp = client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            throw ex;
        }
        
        //DebugUtils.dumpResponseBody(resp);
        
        InputStream is = null;
        InputStreamReader rd = null;
        
        try
        {
            is = resp.getEntity().getContent();
            rd = new InputStreamReader(is);

            LatestLidsResponseParser parser = new LatestLidsResponseParser();
            parser.parse(rd);            
            return parser.getLidvids();
        }
        finally
        {
            CloseUtils.close(rd);
            CloseUtils.close(is);
        }
    }

    
}

