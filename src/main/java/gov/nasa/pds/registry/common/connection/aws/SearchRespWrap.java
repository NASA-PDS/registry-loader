package gov.nasa.pds.registry.common.connection.aws;

import org.opensearch.client.opensearch.core.SearchResponse;
import gov.nasa.pds.registry.common.Response;

class SearchRespWrap implements Response.Search {
  final private SearchResponse<Object> parent;
  SearchRespWrap(SearchResponse<Object> parent) {
    this.parent = parent;
  }
}
