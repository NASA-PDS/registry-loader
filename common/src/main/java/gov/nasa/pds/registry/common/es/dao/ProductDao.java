package gov.nasa.pds.registry.common.es.dao;

import java.util.Collection;
import java.util.List;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.RestClient;

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
        Response.Get resp = null;
        
        try
        {
            resp = client.performRequest(req);
            return resp.productClass();
        }
        catch(ResponseException ex)
        {
            int code = ex.statusCode();
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
        
        // Request URL
        Request.Count req = client.createCountRequest()
            .setIndex(this.indexName + "-refs")
            .setQuery(collectionLidVid, strType);
        try
        {
            return (int)client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            int code = ex.statusCode();
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
        try
        {
            return client.performRequest(req).refs();
        }
        catch(ResponseException ex)
        {
            int code = ex.statusCode();
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
    }
    
    
    /**
     * Update archive status by lidvids
     * @param lidvids a list of lidvids to update
     * @param status new status
     * @throws Exception an exception
     */
    public void updateArchiveStatus(Collection<String> lidvids, String status) throws Exception
    {
        if(lidvids == null || status == null || lidvids.size() == 0) return;
        
        Request.Bulk req = client.createBulkRequest()
            .buildUpdateStatus(lidvids, status)
            .setIndex(this.indexName);
        client.performRequest(req).logErrors();;
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
        try
        {
            return new LidvidSet(client.performRequest(req).ids());
        }
        catch(ResponseException ex)
        {
            int code = ex.statusCode();
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
        try
        {
            return client.performRequest(req).latestLidvids();
        }
        catch(ResponseException ex)
        {
            throw ex;
        }
    }

    
}

