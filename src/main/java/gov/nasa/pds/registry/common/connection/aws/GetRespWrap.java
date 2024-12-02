package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.core.GetResponse;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.util.Tuple;

class GetRespWrap implements Response.Get {
  final private GetResponse<Object> parent;
  final protected Logger log;
  GetRespWrap(GetResponse<Object> parent) {
    this.log = LogManager.getLogger(this.getClass());
    this.parent = parent;
  }
  @Override
  public List<Tuple> dataTypes(boolean stringForMissing)
      throws IOException, DataTypeNotFoundException {
    throw new RuntimeException("This method is supported via MGet and should never be called here");
  }
  @Override
  public IdSets ids() {
    IdSetsImpl result = new IdSetsImpl();
    if (this.parent.source() != null) {
      Map<String,List<String>> src = (Map<String,List<String>>)this.parent.source();
      if (src.containsKey("ref_lid_collection")) result.lids.addAll(src.get("ref_lid_collection"));
      if (src.containsKey("ref_lidvid_collection")) result.lids.addAll(src.get("ref_lidvid_collection"));
    }
    return result;
  }
  @Override
  public String productClass() {
    String result = "";
    if (this.parent.source() != null) {
      Map<String,String> src = (Map<String,String>)this.parent.source();
      if (src.containsKey("product_class")) result = src.get("product_class");
    }
    return result;
  }
  @Override
  public List<String> refs() {
    ArrayList<String> results = new ArrayList<String>();
    if (this.parent.source() != null) {
      Map<String, List<String>> src = (Map<String,List<String>>)this.parent.source();
      if (src.containsKey("product_lidvid")) {
        results.addAll(src.get("product_lidvid"));
      }
    }
    return results;
  }
}
