package gov.nasa.pds.registry.common.es.dao.dd;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaRequestBuilder;
import gov.nasa.pds.registry.common.util.SearchResponseParser;
import gov.nasa.pds.registry.common.util.Tuple;


/**
 * Data dictionary DAO (Data Access Object).
 * This class provides methods to read and update data dictionary. 
 * @author karpenko
 */
public class DataDictionaryDao
{
    private Logger log;

    private RestClient client;
    private String indexName;

    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     */
    public DataDictionaryDao(RestClient client, String indexName)
    {
        log = LogManager.getLogger(this.getClass());

        this.client = client;
        this.indexName = indexName;
    }

    
    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class GetLddInfoRespParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public LddVersions info;
        
        public GetLddInfoRespParser()
        {
            info = new LddVersions();
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                String strDate = (String)map.get("date");
                info.updateDate(strDate);
                
                String file = (String)map.get("attr_name");
                info.addSchemaFile(file);
            }
        }
    }
    

    /**
     * Get LDD date from data dictionary index in Elasticsearch.
     * @param namespace LDD namespace, e.g., "pds", "geom", etc.
     * @return ISO instant class representing LDD date.
     * @throws Exception an exception
     */
    public LddVersions getLddInfo(String namespace) throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder();
        String json = bld.createListLddsRequest(namespace);

        Request req = client.createRequest(Request.Method.GET, "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
        
        GetLddInfoRespParser parser = new GetLddInfoRespParser();
        parser.parseResponse(resp, parser); 
        return parser.info;
    }


    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class ListLddsParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public List<LddInfo> list;
        
        public ListLddsParser()
        {
            list = new ArrayList<>();
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                LddInfo info = new LddInfo();
                
                // Namespace
                info.namespace = (String)map.get("attr_ns");
                
                // Date
                String str = (String)map.get("date");
                if(str != null && !str.isEmpty())
                {
                    info.date = Instant.parse(str);
                }
                
                // Versions
                info.imVersion = (String)map.get("im_version");
                
                // File name
                info.file = (String)map.get("attr_name");
                
                list.add(info);                
            }
        }
    }

    
    /**
     * List registered LDDs
     * @param namespace if this parameter is null list all LDDs
     * @return a list of LDDs
     * @throws Exception an exception
     */
    public List<LddInfo> listLdds(String namespace) throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder();
        String json = bld.createListLddsRequest(namespace);

        Request req = client.createRequest(Request.Method.GET, "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);

        ListLddsParser parser = new ListLddsParser();
        parser.parseResponse(resp, parser); 
        return parser.list;
    }

    
    
    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class ListFieldsParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public Set<String> list;
        
        public ListFieldsParser()
        {
            list = new HashSet<>(200);
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                String fieldName = (String)map.get("es_field_name");
                list.add(fieldName);                
            }
        }
    }

    
    /**
     * Get field names by Elasticsearch type, such as "boolean" or "date".
     * @return a set of field names
     * @throws Exception an exception
     */
    public Set<String> getFieldNamesByEsType(String esType) throws Exception
    {
        DDRequestBuilder bld = new DDRequestBuilder();
        String json = bld.createListFieldsRequest(esType);

        Request req = client.createRequest(Request.Method.GET, "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);

        ListFieldsParser parser = new ListFieldsParser();
        parser.parseResponse(resp, parser); 
        return parser.list;
    }

    
    /**
     * Query Elasticsearch data dictionary to get data types for a list of field ids.
     * @param ids A list of field IDs, e.g., "pds:Array_3D/pds:axes".
     * @param stringForMissing If true, throw DataTypeNotFoundException on first 
     * field missing from Elasticsearch data dictionary. 
     * If false, process all missing fields in a batch to create a list of 
     * missing namespaces. Don't throw DataTypeNotFoundException.  
     * @return Data types information object
     * @throws Exception DataTypeNotFoundException, IOException, etc.
     */
    public List<Tuple> getDataTypes(Collection<String> ids, boolean stringForMissing) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;
        
        List<Tuple> dtInfo = new ArrayList<Tuple>();
        
        // Create request
        Request req = client.createRequest(Request.Method.GET, "/" + indexName + "-dd/_mget?_source=es_data_type");
        
        // Create request body
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createMgetRequest(ids);
        req.setJsonEntity(json);
        
        // Call ES
        Response resp = client.performRequest(req);
        GetDataTypesResponseParser parser = new GetDataTypesResponseParser();
        List<GetDataTypesResponseParser.Record> records = parser.parse(resp.getEntity());
        
        // Process response (list of fields)
        boolean missing = false;
        
        for(GetDataTypesResponseParser.Record rec: records)
        {
            if(rec.found)
            {
                dtInfo.add(new Tuple(rec.id, rec.esDataType));
            }
            // There is no data type for this field in ES registry-dd index
            else
            {
                // Automatically assign data type for known fields
                if(rec.id.startsWith("ref_lid_") || rec.id.startsWith("ref_lidvid_") 
                        || rec.id.endsWith("_Area")) 
                {
                    dtInfo.add(new Tuple(rec.id, "keyword"));
                    continue;
                }
                
                if(stringForMissing) 
                {
                    log.warn("Could not find datatype for field " + rec.id + ". Will use 'keyword'");
                    dtInfo.add(new Tuple(rec.id, "keyword"));
                }
                else
                {
                    log.error("Could not find datatype for field " + rec.id);
                    missing = true;
                }
            }
        }
        
        if(stringForMissing == false && missing == true) throw new DataTypeNotFoundException();
        
        return dtInfo;
    }

}

