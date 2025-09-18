package gov.nasa.pds.registry.common.connection.es;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.es.dao.DaoUtils;
import gov.nasa.pds.registry.common.es.dao.LidvidSet;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.es.dao.dd.GetDataTypesResponseParser;
import gov.nasa.pds.registry.common.util.LidVidUtils;
import gov.nasa.pds.registry.common.util.Tuple;

class GetRespImpl implements Response.Get {
  final private Logger log;
  final private org.elasticsearch.client.Response response;
  GetRespImpl(org.elasticsearch.client.Response response) {
    this.log = LogManager.getLogger(this.getClass());
    this.response = response;
  }
  private LidvidSet parseCollectionIdsSource(JsonReader rd) throws IOException {
    LidvidSet ids = new LidvidSet(null,null);
    rd.beginObject();
    while (rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
      String name = rd.nextName();
      if ("ref_lid_collection".equals(name)) {
        ids.lids = DaoUtils.parseSet(rd);
      } else if ("ref_lidvid_collection".equals(name)) {
        ids.lidvids = DaoUtils.parseSet(rd);
      } else {
        rd.skipValue();
      }
    }
    rd.endObject();
    return ids;
  }
  private String parseProductClassSource(JsonReader rd) throws IOException {
    rd.beginObject();
    while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
      String name = rd.nextName();
      if("product_class".equals(name)) {
        return rd.nextString();
      } else {
        rd.skipValue();
      }
    }
    rd.endObject();
    return null;
  }
  private List<String> parseRefs(JsonReader rd) throws IOException {
    rd.beginObject();
    while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
      String name = rd.nextName();
      if("product_lidvid".equals(name)) {
        return DaoUtils.parseList(rd);
      } else {
        rd.skipValue();
      }
    }
    rd.endObject();
    return null;
  }
  @Override
  public String productClass() {
    try (JsonReader rd = new JsonReader(new InputStreamReader(this.response.getEntity().getContent()))) {
      rd.beginObject();
      while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
        String name = rd.nextName();
        if("_source".equals(name)) {
          return parseProductClassSource(rd);
        } else {
          rd.skipValue();
        }
      }
      rd.endObject();
    } catch (UnsupportedOperationException | IOException e) {
      throw new RuntimeException("Weird JSON parsing error because should never reach this branch");
    }
    return null;
  }
  @Override
  public List<String> refs() {
    try (JsonReader rd = new JsonReader(new InputStreamReader(this.response.getEntity().getContent()))) {
      rd.beginObject();
      while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
        String name = rd.nextName();
        if("_source".equals(name)) {
          return parseRefs(rd);
        } else {
          rd.skipValue();
        }
      }
      rd.endObject();
      } catch (UnsupportedOperationException | IOException e) {
        throw new RuntimeException("Weird JSON parsing problem that should never occur");
      }
    return null;
  }
  @Override
  public IdSets ids() {
    LidvidSet collectionIds = null;
    try (JsonReader rd = new JsonReader(new InputStreamReader(this.response.getEntity().getContent()))) {
      rd.beginObject();
      while (rd.hasNext() && rd.peek() != JsonToken.END_OBJECT) {
        String name = rd.nextName();
        if ("_source".equals(name)) {
          collectionIds = parseCollectionIdsSource(rd);
        } else {
          rd.skipValue();
        }
      }
      rd.endObject();
    } catch (UnsupportedOperationException | IOException e) {
      throw new RuntimeException("Weird JSON parsing problem that should never get here");
    }
    if (collectionIds == null || collectionIds.lidvids == null || collectionIds.lids == null)
      return collectionIds;
    // Harvest converts LIDVIDs to LIDs, so let's delete those converted LIDs.
    for (String lidvid : collectionIds.lidvids) {
      String lid = LidVidUtils.lidvidToLid(lidvid);
      if (lid != null) {
        collectionIds.lids.remove(lid);
      }
    }
    return collectionIds;
  }
  @Override
  public List<Tuple> dataTypes(boolean stringForMissing) throws IOException, DataTypeNotFoundException {
    List<Tuple> dtInfo = new ArrayList<Tuple>();
    GetDataTypesResponseParser parser = new GetDataTypesResponseParser();
    List<GetDataTypesResponseParser.Record> records = parser.parse(this.response.getEntity());
    // Process response (list of fields)
    boolean missing = false;
    for (GetDataTypesResponseParser.Record rec : records) {
      if (rec.found) {
        dtInfo.add(new Tuple(rec.id, rec.esDataType));
      }
      // There is no data type for this field in ES registry-dd index
      else {
        // Automatically assign data type for known fields
        if (rec.id.startsWith("ref_lid_") || rec.id.startsWith("ref_lidvid_")
            || rec.id.endsWith("_Area")) {
          dtInfo.add(new Tuple(rec.id, "keyword"));
          continue;
        }
        if (stringForMissing) {
          log.warn("Could not find datatype for field " + rec.id + ". Will use 'keyword'");
          dtInfo.add(new Tuple(rec.id, "keyword"));
        } else {
          log.error("Could not find datatype for field " + rec.id);
          missing = true;
        }
      }
    }
    if (stringForMissing == false && missing == true)
      throw new DataTypeNotFoundException();

    return dtInfo;
  }
}
