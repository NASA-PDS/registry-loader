package gov.nasa.pds.registry.common;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.es.dao.dd.LddInfo;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;
import gov.nasa.pds.registry.common.util.Tuple;

public interface Response {
  public interface Bulk {
    public interface Item {
      public boolean error();
      public String id();
      public String index();
      public String operation();
      public String reason();
      public String result();
      public int status();
    }
    public boolean errors();
    public List<Item> items();
    public void logErrors();
    public long took();
  }
  public interface CreatedIndex {
    public boolean acknowledge();
    public boolean acknowledgeShards();
    public String getIndex();
  }
  public interface Get {
    public interface IdSets {
      public Set<String> lids();
      public Set<String> lidvids();
    }
    public List<Tuple> dataTypes() throws IOException, DataTypeNotFoundException;    
    public IdSets ids(); // returns null if nothing is found in returned content
    public String productClass(); // returns null if product class not in returned content
    public List<String> refs(); // returns null if nothing is found in returned content
  }
  public interface Mapping {
    public Set<String> fieldNames();
  }
  public interface Search {
    public Map<String,Set<String>> altIds() throws UnsupportedOperationException, IOException;
    public List<Object> batch() throws UnsupportedOperationException, IOException;
    public List<String> bucketValues();
    public List<Map<String,Object>> documents();
    public String field(String name) throws NoSuchFieldException; // null means blob not in document and NoSuchFieldException document not found
    public Set<String> fields() throws UnsupportedOperationException, IOException;
    public List<String> lidvids(); // returns empty list if nothing is found in returned content
    public List<String> latestLidvids(); // returns null if nothing is found in returned content
    public LddVersions lddInfo() throws UnsupportedOperationException, IOException;
    public List<LddInfo> ldds() throws UnsupportedOperationException, IOException;
    public Set<String> nonExistingIds(Collection<String> from_ids) throws UnsupportedOperationException, IOException;
  }
  public interface Settings {
    public int replicas();
    public int shards();
  }
}
