package gov.nasa.pds.registry.common.connection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.core.SearchRequest;

/**
 * The data dictionary can hold well over 1000 fields of a given type, so the "list fields" request
 * must not be limited to the old hardcoded page size of 1000.
 */
class TestSearchImplListFields {

  @Test
  void buildListFields_requestsFullResultWindow() {
    SearchImpl search = new SearchImpl();
    search.buildListFields("date").setIndex("registry-dd");

    SearchRequest request = search.craftsman.build();

    assertEquals(SearchImpl.MAX_LIST_FIELDS_SIZE, request.size());
    assertTrue(SearchImpl.MAX_LIST_FIELDS_SIZE > 1000);
  }

  @Test
  void buildListFields_tracksTotalHitsSoTruncationCanBeDetected() {
    SearchImpl search = new SearchImpl();
    search.buildListFields("date").setIndex("registry-dd");

    SearchRequest request = search.craftsman.build();

    assertTrue(request.trackTotalHits() != null && Boolean.TRUE.equals(request.trackTotalHits().enabled()));
  }
}
