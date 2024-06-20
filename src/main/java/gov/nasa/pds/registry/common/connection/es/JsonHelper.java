package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Collection;
import org.apache.http.HttpEntity;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.Tuple;

/**
 * Builds Elasticsearch JSON queries
 * 
 * @author karpenko
 */
class JsonHelper {
  /**
   * Build update product archive status JSON request
   * 
   * @param lidvids list of LIDVIDs to update
   * @param status new status
   * @return JSON
   */
  static String buildUpdateStatusJson(Collection<String> lidvids, String status) {
    if (lidvids == null || lidvids.isEmpty())
      return null;
    if (status == null || status.isEmpty())
      throw new IllegalArgumentException("Status could not be null or empty.");

    StringBuilder bld = new StringBuilder();
    String dataLine =
        "{ \"doc\" : {\"" + Metadata.FLD_ARCHIVE_STATUS + "\" : \"" + status + "\"} }\n";

    // Build NJSON (new-line delimited JSON)
    for (String lidvid : lidvids) {
      // Line 1: Elasticsearch document ID
      bld.append("{ \"update\" : {\"_id\" : \"" + lidvid + "\" } }\n");
      // Line 2: Data
      bld.append(dataLine);
    }

    return bld.toString();
  }


  /**
   * Build aggregation query to select latest versions of lids
   * 
   * @param lids list of LIDs
   * @return JSON
   */
  static String buildGetLatestLidVidsJson(Collection<String> lids) {
    if (lids == null || lids.isEmpty())
      return null;

    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
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
      for (String lid : lids) {
        jw.value(lid);
      }
      jw.endArray();

      jw.endObject(); // terms
      jw.endObject(); // query

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

      jw.endObject(); // top_hits
      jw.endObject(); // latest
      jw.endObject(); // aggs

      jw.endObject(); // lids
      jw.endObject(); // aggs

      jw.endObject();

      return strWriter.toString();
    } catch (IOException ioe) {
      // not sure how we got here but fail in a large way
      throw new RuntimeException("some weird internal JSON writing problem");
    }
  }

  static String buildSearchIdsRequest(Collection<String> ids, int pageSize, boolean isAlt) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter writer = new JsonWriter(strWriter)) {
      // Create ids query
      writer.beginObject();

      // Exclude source from response
      JsonWriter src = writer.name("_source");
      if (isAlt)
        src.value("alternate_ids");
      else
        src.value(false);
      writer.name("size").value(pageSize);

      writer.name("query");
      writer.beginObject();
      writer.name("ids");
      writer.beginObject();

      writer.name("values");
      JsonWriter a = writer.beginArray();
      for (String id : ids) {
        writer.value(id);
      }
      writer.endArray();

      writer.endObject();
      writer.endObject();
      writer.endObject();

      a.close();
      writer.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }
    return strWriter.toString();
  }

  static String buildListLddsRequest(String namespace) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
      jw.beginObject();
      // Size (number of records to return)
      jw.name("size").value(1000);

      // Start query
      jw.name("query");
      jw.beginObject();
      jw.name("bool");
      jw.beginObject();

      jw.name("must");
      jw.beginArray();
      appendMatch(jw, "class_ns", "registry");
      appendMatch(jw, "class_name", "LDD_Info");
      if (namespace != null) {
        appendMatch(jw, "attr_ns", namespace);
      }
      jw.endArray();

      jw.endObject();
      jw.endObject();
      // End query

      // Start source
      jw.name("_source");
      jw.beginArray();
      jw.value("date").value("attr_name").value("attr_ns").value("im_version");
      jw.endArray();
      // End source

      jw.endObject();
      jw.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }

    return strWriter.toString();
  }

  static String buildListFieldsRequest(String dataType) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
      jw.beginObject();
      // Size (number of records to return)
      jw.name("size").value(1000);

      // Start query
      jw.name("query");
      jw.beginObject();
      jw.name("bool");
      jw.beginObject();

      jw.name("must");
      jw.beginArray();
      appendMatch(jw, "es_data_type", dataType);
      jw.endArray();

      jw.endObject();
      jw.endObject();
      // End query

      // Start source
      jw.name("_source");
      jw.beginArray();
      jw.value("es_field_name");
      jw.endArray();
      // End source

      jw.endObject();
      jw.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }

    return strWriter.toString();
  }


  /**
   * Create get data dictionary (LDD) info request.
   * 
   * @param namespace LDD namespace ID, such as 'pds', 'cart', etc.
   * @return Elasticsearch query in JSON format
   * @throws IOException an exception
   */
  static String buildGetLddInfoRequest(String namespace) throws IOException {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
      jw.beginObject();
      // Size (number of records to return)
      jw.name("size").value(1000);

      // Start query
      jw.name("query");
      jw.beginObject();
      jw.name("bool");
      jw.beginObject();

      jw.name("must");
      jw.beginArray();
      appendMatch(jw, "class_ns", "registry");
      appendMatch(jw, "class_name", "LDD_Info");
      appendMatch(jw, "attr_ns", namespace);
      jw.endArray();

      jw.endObject();
      jw.endObject();
      // End query

      // Start source
      jw.name("_source");
      jw.beginArray();
      jw.value("date").value("attr_name");
      jw.endArray();
      // End source

      jw.endObject();
      jw.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }

    return strWriter.toString();
  }


  private static void appendMatch(JsonWriter jw, String field, String value) throws IOException {
    jw.beginObject();
    jw.name("match");
    jw.beginObject();
    jw.name(field).value(value);
    jw.endObject();
    jw.endObject();
  }

  static String buildIdList(Collection<String> ids) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
      jw.beginObject();
      jw.name("ids");

      JsonWriter a = jw.beginArray();
      for (String id : ids) {
        jw.value(id);
      }
      jw.endArray();

      jw.endObject();
      a.close();
      jw.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }

    return strWriter.toString();
  }

  static String buildUpdateSchemaRequest(Collection<Tuple> fields) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter jw = new JsonWriter(strWriter)) {
      jw.beginObject();

      jw.name("properties");
      JsonWriter o = jw.beginObject();
      for (Tuple field : fields) {
        jw.name(field.item1);
        jw.beginObject();
        jw.name("type").value(field.item2);
        jw.endObject();
      }
      jw.endObject();

      jw.endObject();
      o.close();
      jw.close();
    } catch (IOException ioe) {
      // cannot imagine ever getting here
      throw new RuntimeException(
          "some weird JSON serialization problem that should not be happening");
    }

    return strWriter.toString();
  }

  static long findCount (HttpEntity entity) {
    long count = 0;
    try (InputStream is = entity.getContent()) {
      JsonReader rd = new JsonReader(new InputStreamReader(is));  
      rd.beginObject();
      while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
        String name = rd.nextName();
        if("count".equals(name)) {
          count = rd.nextInt();
          break;
        } else {
          rd.skipValue();
        }
      }
      rd.endObject();
    } catch (UnsupportedOperationException | IOException e) {
      throw new RuntimeException("some weird JSON serialization problem that should not be happening");
    }
    return count;
  }
}
