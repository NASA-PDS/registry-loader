package gov.nasa.pds.registry.common;

import java.util.Collection;
import java.util.List;
import gov.nasa.pds.registry.common.util.Tuple;

public interface Request {
  public interface Bulk { // _bulk
    enum Refresh { False, True, WaitFor };
    public void add (String statement, String document); // create, index, update
    public Bulk buildUpdateStatus(Collection<String> lidvids, String status);
    public Bulk setIndex(String name);
    public Bulk setRefresh(Refresh type);
  }
  public interface Count { // _count
    public Count setIndex (String name);
    public Count setQuery (String q);
  }
  public interface Delete { // -X DELETE _doc
    public Delete setDocId(String id);
    public Delete setIndex (String name);
  }
  public interface DeleteByQuery { // _delete_by_query is not directly supported in AOSS
    public DeleteByQuery createFilterQuery(String key, String value);
    public DeleteByQuery createMatchAllQuery();
    public DeleteByQuery setIndex (String name);
    public DeleteByQuery setRefresh(boolean state);
  }
  public interface Get { // _doc
    public Get excludeField (String field);
    public Get excludeFields (List<String> fields);
    public Get includeField (String field);
    public Get includeFields (List<String> fields);
    public Get setId (String id);
    public Get setIndex (String index);
  }
  public interface Mapping { // _mapping
    public Mapping buildUpdateFieldSchema (Collection<Tuple> pairs);
    public Mapping setIndex(String name);
  }
  public interface MGet extends Get { // _mget
    public MGet setIds (Collection<String> ids);
  }
  public interface Search { // _search
    public Search all(String sortField, int size, String searchAfter);
    public Search all(String filterField, String filterValue, String sortField, int size, String searchAfter);
    public Search buildAlternativeIds(Collection<String> lids);
    public Search buildGetField(String field_name, String lidvid);
    public Search buildLatestLidVids(Collection<String> lids);
    public Search buildListFields(String dataType);
    public Search buildListLdds (String namespace);
    public Search buildTermQuery (String fieldname, String value);
    public Search buildTheseIds(Collection<String> lids);
    public Search setIndex (String name);
    public Search setPretty (boolean pretty);
    public Search setSize (int hitsperpage);
  }
  public interface Setting { // _settings
    public Setting setIndex (String name);
  }
}
