package gov.nasa.pds.registry.common.connection.aws;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.client.opensearch.core.MgetResponse;
import org.opensearch.client.opensearch.core.mget.MultiGetResponseItem;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.util.Tuple;

class MGetRespWrap extends GetRespWrap {
  final private MgetResponse<Object> parent;
  MGetRespWrap (MgetResponse<Object> parent) {
    super(null);
    this.parent = parent;
  }
  @Override
  public List<Tuple> dataTypes(boolean stringForMissing) throws IOException, DataTypeNotFoundException {
    ArrayList<Tuple> results = new ArrayList<Tuple>();
    for (MultiGetResponseItem<Object> doc : this.parent.docs()) {
      Tuple t = null;
      if (doc.isResult()) {
        @SuppressWarnings("unchecked")
        Map<String,String> src = (Map<String,String>)doc.result().source();
        if (src != null && src.containsKey("es_data_type")) {
          t = new Tuple(doc.result().id(), src.get("es_data_type"));
        } else if (stringForMissing) {
          if (doc.result().id().startsWith("ref_lid_") || doc.result().id().startsWith("ref_lidvid_")
              || doc.result().id().endsWith("_Area")) {
            t = new Tuple(doc.result().id(), "keyword");
          } else {
            log.warn("Could not find datatype for field {} and defaulting to 'text'", doc.result().id());
            t = new Tuple(doc.result().id(), "text");
          }
        } else {
          throw new DataTypeNotFoundException();
        }
      }
      if (t != null) results.add(t);
    }
    return results;
  }
}
