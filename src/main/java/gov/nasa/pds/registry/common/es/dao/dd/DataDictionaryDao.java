package gov.nasa.pds.registry.common.es.dao.dd;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.RestClient;
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
        // Create request
        Request.Get req = client.createMGetRequest()
            .setIds(ids)
            .includeField("es_data_type")
            .setIndex(this.indexName + "-dd");
        return this.client.performRequest(req).dataTypes(stringForMissing);
    }

}

