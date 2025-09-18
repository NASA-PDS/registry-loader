package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import com.google.gson.stream.JsonWriter;

/**
 * A class to build Elasticsearch API JSON requests.
 * 
 * @author karpenko
 */
class RegistryRequestBuilder
{
    private boolean pretty;

    
    /**
     * Constructor
     * @param pretty Pretty-format JSON requests
     */
    RegistryRequestBuilder(boolean pretty)
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
     * Build export data request
     * @param filterField Filter field name, such as "lidvid".
     * @param filterValue Filter value.
     * @param sortField Sort field is required to paginate data and use "search_after" field.
     * @param size Batch / page size
     * @param searchAfter "search_after" field to perform pagination
     * @return JSON
     * @throws IOException an exception
     */
    public String createExportDataRequest(String filterField, String filterValue, String sortField, int size, String searchAfter) {
      StringWriter out = new StringWriter();
      try (JsonWriter writer = createJsonWriter(out)) {
      writer.beginObject();
      // Size (number of records to return)
      writer.name("size").value(size);
      // Filter query
      EsQueryUtils.appendFilterQuery(writer, filterField, filterValue);
      // "search_after" parameter is used for pagination
      if (searchAfter != null) {
        writer.name("search_after").value(searchAfter);
      }
      // Sort is required by pagination
      writer.name("sort");
      writer.beginObject();
      writer.name(sortField).value("asc");
      writer.endObject();
      writer.endObject();
      writer.close();
      } catch (IOException e) {
        throw new RuntimeException("Should never get here");
      }
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
    public String createExportAllDataRequest(String sortField, int size, String searchAfter) {
      StringWriter out = new StringWriter();
      try (JsonWriter writer = createJsonWriter(out)) {
        writer.beginObject();
        // Size (number of records to return)
        writer.name("size").value(size);
        // Match all query
        EsQueryUtils.appendMatchAllQuery(writer);
        // "search_after" parameter is used for pagination
        if (searchAfter != null) {
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
      } catch (IOException e) {
        throw new RuntimeException("Should never get here.");
      }
      return out.toString();
    }

    
    /**
     * Build get BLOB request 
     * @param lidvid a LidVid
     * @return JSON
     * @throws IOException an exception
     */
    public String createGetBlobRequest(String fieldName, String lidvid)
    {
        StringWriter out = new StringWriter();
        try (JsonWriter writer = createJsonWriter(out)){

        writer.beginObject();

        // Return only BLOB
        writer.name("_source");
        writer.beginArray();
        writer.value(fieldName);
        writer.endArray();

        // Query
        EsQueryUtils.appendFilterQuery(writer, "lidvid", lidvid);
        writer.endObject();

        writer.close();
        } catch (IOException e) {
          throw new RuntimeException("Should never get here");
        }
        return out.toString();
    }


    /**
     * Create Elasticsearch filter query
     * @param field filter field name
     * @param value filter value
     * @return JSON
     * @throws IOException an exception
     */
    public String createFilterQuery(String field, String value)
    {
        StringWriter out = new StringWriter();
        try (JsonWriter writer = createJsonWriter(out)) {

        writer.beginObject();
        EsQueryUtils.appendFilterQuery(writer, field, value);
        writer.endObject();

        writer.close();
        } catch (IOException e) {
          throw new RuntimeException("Should never get here");
        }
        return out.toString();
    }

    
    /**
     * Build match all query
     * @return JSON
     * @throws IOException an exception
     */
    public String createMatchAllQuery()
    {
        StringWriter out = new StringWriter();
        try (JsonWriter writer = createJsonWriter(out)) {

        writer.beginObject();

        writer.name("query");
        writer.beginObject();
        EsQueryUtils.appendMatchAll(writer);
        writer.endObject();

        writer.endObject();

        writer.close();
        } catch (IOException e) {
          throw new RuntimeException("Should never get here");
        }
        return out.toString();
    }
}
