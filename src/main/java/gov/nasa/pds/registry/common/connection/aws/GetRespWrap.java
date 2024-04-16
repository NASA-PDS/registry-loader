package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.core.GetResponse;
import gov.nasa.pds.registry.common.Response;

class GetRespWrap implements Response.Get {
  final private GetResponse<Object> parent;
  GetRespWrap(GetResponse<Object> parent) {
    this.parent = parent;
  }
}
