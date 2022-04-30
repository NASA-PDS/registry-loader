package gov.nasa.pds.registry.mgr.dao;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.es.EsQueryUtils;

/**
 * A class to build Elasticsearch API JSON requests.
 * 
 * @author karpenko
 */
public class RegistryRequestBuilder
{
    private boolean pretty;

    
    /**
     * Constructor
     * @param pretty Pretty-format JSON requests
     */
    public RegistryRequestBuilder(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    /**
     * Constructor
     */
    public RegistryRequestBuilder()
    {
        this(false);
    }

    
    private JsonWriter createJsonWriter(Writer writer)
    {
        JsonWriter jw = new JsonWriter(writer);
        if (pretty)
        {
            jw.setIndent("  ");
        }

        return jw;
    }

    
    /**
     * Build create index request
     * @param schemaFile index schema file
     * @param shards number of shards
     * @param replicas number of replicas
     * @return JSON
     * @throws Exception Generic exception
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public String createCreateIndexRequest(File schemaFile, int shards, int replicas) throws Exception
    {
        // Read schema template
        FileReader rd = new FileReader(schemaFile);
        Gson gson = new Gson();
        Object rootObj = gson.fromJson(rd, Object.class);
        CloseUtils.close(rd);

        Object settingsObj = ((Map)rootObj).get("settings");
        if (settingsObj == null)
        {
            settingsObj = new TreeMap();
        }

        Object mappingsObj = ((Map)rootObj).get("mappings");
        if (mappingsObj == null)
        {
            throw new Exception("Missing mappings in schema file " + schemaFile.getAbsolutePath());
        }

        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        Map settingsMap = (Map)settingsObj;
        settingsMap.put("number_of_shards", shards);
        settingsMap.put("number_of_replicas", replicas);

        // Settings
        writer.name("settings");
        gson.toJson(settingsObj, Object.class, writer);

        // Mappings
        writer.name("mappings");
        gson.toJson(mappingsObj, Object.class, writer);

        writer.endObject();

        writer.close();
        return out.toString();
    }


    /**
     * Build export data request
     * @param filterField Filter field name, such as "lidvid".
     * @param filterValue Filter value.
     * @param sortField Sort field is required to paginate data and use "search_after" field.
     * @param size Batch / page size
     * @param searchAfter "search_after" field to perform pagination
     * @return JSON
     * @throws IOException an exception
     */
    public String createExportDataRequest(String filterField, String filterValue, 
            String sortField, int size, String searchAfter) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        // Size (number of records to return)
        writer.name("size").value(size);

        // Filter query
        EsQueryUtils.appendFilterQuery(writer, filterField, filterValue);

        // "search_after" parameter is used for pagination
        if (searchAfter != null)
        {
            writer.name("search_after").value(searchAfter);
        }

        // Sort is required by pagination
        writer.name("sort");
        writer.beginObject();
        writer.name(sortField).value("asc");
        writer.endObject();

        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    /**
     * Build export all data request
     * @param sortField Sort field is required to paginate data and use "search_after" field. 
     * @param size Batch / page size
     * @param searchAfter "search_after" field to perform pagination
     * @return JSON
     * @throws IOException an exception
     */
    public String createExportAllDataRequest(String sortField, int size, String searchAfter) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        // Size (number of records to return)
        writer.name("size").value(size);

        // Match all query
        EsQueryUtils.appendMatchAllQuery(writer);

        // "search_after" parameter is used for pagination
        if (searchAfter != null)
        {
            writer.name("search_after");
            writer.beginArray();
            writer.value(searchAfter);
            writer.endArray();
        }

        // Sort is required by pagination
        writer.name("sort");
        writer.beginObject();
        writer.name(sortField).value("asc");
        writer.endObject();

        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    /**
     * Build get BLOB request 
     * @param lidvid a LidVid
     * @return JSON
     * @throws IOException an exception
     */
    public String createGetBlobRequest(String lidvid) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        // Return only BLOB
        writer.name("_source");
        writer.beginArray();
        writer.value(Constants.BLOB_FIELD);
        writer.endArray();

        // Query
        EsQueryUtils.appendFilterQuery(writer, "lidvid", lidvid);
        writer.endObject();

        writer.close();
        return out.toString();
    }


    /**
     * Create Elasticsearch filter query
     * @param field filter field name
     * @param value filter value
     * @return JSON
     * @throws IOException an exception
     */
    public String createFilterQuery(String field, String value) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();
        EsQueryUtils.appendFilterQuery(writer, field, value);
        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    /**
     * Build match all query
     * @return JSON
     * @throws IOException an exception
     */
    public String createMatchAllQuery() throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        writer.name("query");
        writer.beginObject();
        EsQueryUtils.appendMatchAll(writer);
        writer.endObject();

        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    /**
     * Build a query to select alternate ids by document primary key
     * @param ids list of primary keys (lidvids right now)
     * @return JSON
     * @throws Exception an exception
     */
    public String createGetAlternateIdsRequest(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) throw new Exception("Missing ids");
            
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        // Create ids query
        writer.beginObject();

        // Exclude source from response
        writer.name("_source").value("alternate_ids");
        writer.name("size").value(ids.size());

        writer.name("query");
        writer.beginObject();
        writer.name("ids");
        writer.beginObject();
        
        writer.name("values");
        writer.beginArray();
        for(String id: ids)
        {
            writer.value(id);
        }
        writer.endArray();
        
        writer.endObject();
        writer.endObject();
        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    public String createUpdateAltIdsRequest(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) throw new IllegalArgumentException("Missing ids");
        
        StringBuilder bld = new StringBuilder();
        
        // Build NJSON (new-line delimited JSON)
        for(Map.Entry<String, Set<String>> entry: newIds.entrySet())
        {
            // Line 1: Elasticsearch document ID
            bld.append("{ \"update\" : {\"_id\" : \"" + entry.getKey() + "\" } }\n");
            
            // Line 2: Data
            String dataJson = buildUpdateDocJson("alternate_ids", entry.getValue());
            bld.append(dataJson);
            bld.append("\n");
        }
        
        return bld.toString();

    }
    
    
    private String buildUpdateDocJson(String field, Collection<String> values) throws Exception
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        writer.name("doc");
        writer.beginObject();
        
        writer.name(field);
        
        writer.beginArray();        
        for(String value: values)
        {
            writer.value(value);
        }
        writer.endArray();
        
        writer.endObject();        
        writer.endObject();
        
        writer.close();
        return out.toString();
    }
}
