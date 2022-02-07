package gov.nasa.pds.registry.common.es.dao;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.CloseUtils;

/**
 * Builds Elasticsearch JSON queries
 * @author karpenko
 */
public class ProductRequestBuilder
{
    /**
     * Build update product archive status JSON request
     * @param lidvids list of LIDVIDs to update
     * @param status new status
     * @return JSON
     */
    public static String buildUpdateStatusJson(Collection<String> lidvids, String status)
    {
        if(lidvids == null || lidvids.isEmpty()) return null;
        if(status == null || status.isEmpty()) throw new IllegalArgumentException("Status could not be null or empty.");
        
        StringBuilder bld = new StringBuilder();
        String dataLine = "{ \"doc\" : {\"" + Metadata.FLD_ARCHIVE_STATUS + "\" : \"" + status + "\"} }\n";
        
        // Build NJSON (new-line delimited JSON)
        for(String lidvid: lidvids)
        {
            // Line 1: Elasticsearch document ID
            bld.append("{ \"update\" : {\"_id\" : \"" + lidvid + "\" } }\n");
            // Line 2: Data
            bld.append(dataLine);
        }
        
        return bld.toString();
    }

    
    /**
     * Build aggregation query to select latest versions of lids
     * @param lids list of LIDs
     * @return JSON
     */
    public static String buildGetLatestLidVidsJson(Collection<String> lids) throws IOException
    {
        if(lids == null || lids.isEmpty()) return null;
        
        JsonWriter jw = null;
        
        try
        {
            StringWriter strWriter = new StringWriter();
            jw = new JsonWriter(strWriter);
            
            jw.beginObject();
    
            jw.name("_source").value(false);
            jw.name("size").value(0);
            
            // Query
            jw.name("query");
            jw.beginObject();
    
            jw.name("terms");
            jw.beginObject();
            
            jw.name("lid");
            jw.beginArray();
            for(String lid: lids)
            {
                jw.value(lid);
            }
            jw.endArray();
    
            jw.endObject();     // terms
            jw.endObject();     // query
                    
            // Aggs
            jw.name("aggs");
            jw.beginObject();
    
            jw.name("lids");
            jw.beginObject();
            
            jw.name("terms");
            jw.beginObject();
            jw.name("field").value("lid");
            jw.name("size").value(5000);
            jw.endObject();
    
            jw.name("aggs");
            jw.beginObject();
            jw.name("latest");
            jw.beginObject();
            jw.name("top_hits");
            jw.beginObject();
    
            jw.name("sort");
            jw.beginArray();
            jw.beginObject();
            jw.name("vid");
            jw.beginObject();
            jw.name("order").value("desc");        
            jw.endObject();
            jw.endObject();
            jw.endArray();
            
            jw.name("_source").value(false);
            jw.name("size").value(1);
    
            jw.endObject();     // top_hits
            jw.endObject();     // latest
            jw.endObject();     // aggs
            
            jw.endObject();     // lids
            jw.endObject();     // aggs
    
            jw.endObject();
            
            return strWriter.toString();
        }
        finally
        {
            CloseUtils.close(jw);
        }
    }

}
