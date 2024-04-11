package gov.nasa.pds.registry.common;

import java.util.List;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;

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
    public long took();
  }
  public interface CreatedIndex {
    public boolean acknowledge();
    public boolean acknowledgeShards();
    public String getIndex();
  }
  public interface Mapping {
    public Set<String> fieldNames();
  }
  public interface Settings {
    public int replicas();
    public int shards();
  }
  public HttpEntity getEntity();
  public StatusLine getStatusLine();
  public void printWarnings();
}
