package gov.nasa.pds.registry.mgr.util.es;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.mgr.util.CloseUtils;

public class EsRequestBuilder
{
    private boolean pretty;

    
    public EsRequestBuilder(boolean pretty)
    {
        this.pretty = pretty;
    }

    
    public EsRequestBuilder()
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

    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public String createCreateRegistryRequest(File schemaFile, int shards, int replicas) throws Exception
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


    public String createExportDataRequest(String field, String value, int size, String searchAfter) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        // Size (number of records to return)
        writer.name("size").value(size);

        // Filter query
        EsQueryUtils.appendFilterQuery(writer, field, value);

        // "search_after" parameter is used for pagination
        if (searchAfter != null)
        {
            writer.name("search_after").value(searchAfter);
        }

        // Sort is required by pagination
        writer.name("sort");
        writer.beginObject();
        writer.name("lidvid").value("asc");
        writer.endObject();

        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    public String createExportAllDataRequest(int size, String searchAfter) throws IOException
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
        writer.name("lidvid").value("asc");
        writer.endObject();

        writer.endObject();

        writer.close();
        return out.toString();
    }

    
    public String createGetBlobRequest(String lidvid) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();

        // Return only BLOB
        writer.name("_source");
        writer.beginArray();
        writer.value("_file_blob");
        writer.endArray();

        // Query
        EsQueryUtils.appendFilterQuery(writer, "lidvid", lidvid);
        writer.endObject();

        writer.close();
        return out.toString();
    }

    
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

    
    public String createUpdateStatusRequest(String status, String field, String value) throws IOException
    {
        StringWriter out = new StringWriter();
        JsonWriter writer = createJsonWriter(out);

        writer.beginObject();
        // Script
        writer.name("script").value("ctx._source.archive_status = '" + status + "'");
        // Query
        EsQueryUtils.appendFilterQuery(writer, field, value);
        writer.endObject();

        writer.close();
        return out.toString();
    }

}
