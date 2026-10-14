package gov.nasa.pds.registry.common.es.dao.dd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.dd.DDRecord;
import gov.nasa.pds.registry.common.dd.LddUtils;
import gov.nasa.pds.registry.common.util.Tuple;


/**
 * Data dictionary DAO (Data Access Object).
 * This class provides methods to read and update data dictionary. 
 * @author karpenko
 */
public class DataDictionaryDao
{
    private RestClient client;
    private String indexName;

    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     */
    public DataDictionaryDao(RestClient client, String indexName)
    {
        this.client = client;
        this.indexName = indexName;
    }

    
    

    /**
     * Get LDD date from data dictionary index in Elasticsearch.
     * @param namespace LDD namespace, e.g., "pds", "geom", etc.
     * @return ISO instant class representing LDD date.
     * @throws Exception an exception
     */
    public LddVersions getLddInfo(String namespace) throws Exception
    {
        Request.Search req = client.createSearchRequest()
            .buildListLdds(namespace)
            .setIndex(indexName + "-dd");
        return client.performRequest(req).lddInfo();
    }

    /**
     * Same as {@link #getLddInfo(String)} but forces a cache bypass (requestCache=false).
     * Use only in targeted wait loops after bulk loading an LDD — do not use in normal query paths.
     * @param namespace LDD namespace, e.g., "pds", "geom", etc.
     * @return ISO instant class representing LDD date.
     * @throws Exception an exception
     */
    public LddVersions getLddInfoNoCache(String namespace) throws Exception
    {
        Request.Search req = client.createSearchRequest()
            .buildListLddsNoCache(namespace)
            .setIndex(indexName + "-dd");
        return client.performRequest(req).lddInfo();
    }


    /**
     * List registered LDDs
     * @param namespace if this parameter is null list all LDDs
     * @return a list of LDDs
     * @throws Exception an exception
     */
    public List<LddInfo> listLdds(String namespace) throws Exception
    {
        Request.Search req = client.createSearchRequest()
            .buildListLdds(namespace)
            .setIndex(this.indexName + "-dd");
        return client.performRequest(req).ldds();
    }

    /**
     * Get field names by Elasticsearch type, such as "boolean" or "date".
     * @return a set of field names
     * @throws Exception an exception
     */
    public Set<String> getFieldNamesByEsType(String esType) throws Exception
    {
        Request.Search req = client.createSearchRequest()
            .buildListFields(esType)
            .setIndex(this.indexName + "-dd");
        return client.performRequest(req).fields();
    }

    
    /**
     * Query Elasticsearch data dictionary to get data types for a list of field ids.
     * @param ids A list of field IDs, e.g., "pds:Array_3D/pds:axes".
     * @return Data types information object
     * @throws DataTypeNotFoundException
     * @throws IOException
     */
    public List<Tuple> getDataTypes(Collection<String> ids) throws IOException, DataTypeNotFoundException
    {
        return getDataTypes(ids, false);
    }

    /**
     * Query Elasticsearch data dictionary to get data types for a list of field ids.
     * @param ids A list of field IDs, e.g., "pds:Array_3D/pds:axes".
     * @param forceRefresh If true, force a shard refresh before the mget. Use only in targeted wait
     *        loops after bulk loading an LDD — do not use in normal query paths.
     * @return Data types information object
     * @throws DataTypeNotFoundException
     * @throws IOException
     */
    public List<Tuple> getDataTypes(Collection<String> ids, boolean forceRefresh) throws IOException, DataTypeNotFoundException
    {
        if(ids == null || ids.isEmpty()) return null;

        HashMap<String,HashSet<String>> mapping = new HashMap<String,HashSet<String>>();
        for (String id : ids) {
          String[] parts = id.split("\\.");
          String typeId = id;
          if (parts.length > 1) {
            typeId = parts[parts.length-2] + "." +  parts[parts.length-1];
          }
          if (!mapping.containsKey(typeId)) mapping.put(typeId, new HashSet<String>());
          mapping.get(typeId).add(id);
        }
        Request.MGet mgetReq = client.createMGetRequest();
        if (forceRefresh) mgetReq.setRefresh(true);
        Request.Get req = mgetReq
            .setIds(mapping.keySet())
            .includeField("es_data_type")
            .setIndex(this.indexName + "-dd");
        ArrayList<Tuple> result = new ArrayList<Tuple>(ids.size());
        for (Tuple t : this.client.performRequest(req).dataTypes()) {
          for (String id : mapping.get(t.item1)) {
            result.add(new Tuple(id, t.item2));
          }
        }
        return result;
    }


    /**
     * Write the LDD_Info sentinel document directly via a single-item bulk request.
     * Call this only after all field documents for the LDD have been successfully ingested,
     * so that a bulk failure on field documents can never leave an orphaned sentinel.
     */
    public void saveLddInfo(String namespace, String lddFileName, String imVersion,
        String lddVersion, String rawDate) throws Exception {
      DDRecord rec = new DDRecord();
      rec.classNs = "registry";
      rec.className = "LDD_Info";
      rec.attrNs = namespace;
      rec.attrName = lddFileName;

      String docId = rec.esFieldNameFromComponents();
      Gson gson = new Gson();

      JsonObject actionBody = new JsonObject();
      actionBody.addProperty("_id", docId);
      JsonObject action = new JsonObject();
      action.add("index", actionBody);

      JsonObject doc = new JsonObject();
      doc.addProperty("es_field_name", docId);
      doc.addProperty("class_ns", rec.classNs);
      doc.addProperty("class_name", rec.className);
      doc.addProperty("attr_ns", rec.attrNs);
      doc.addProperty("attr_name", rec.attrName);
      if (imVersion != null) doc.addProperty("im_version", imVersion);
      if (lddVersion != null) doc.addProperty("ldd_version", lddVersion);
      if (rawDate != null) doc.addProperty("date", LddUtils.lddDateToIsoInstantString(rawDate));

      Request.Bulk bulk = client.createBulkRequest()
          .setRefresh(Request.Bulk.Refresh.WaitFor)
          .setIndex(indexName + "-dd");
      bulk.add(gson.toJson(action), gson.toJson(doc));
      client.performRequest(bulk);
    }

}

