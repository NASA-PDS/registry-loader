package gov.nasa.pds.registry.common.es.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import gov.nasa.pds.registry.common.es.dao.LidvidSet;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.es.service.ProductService;

/**
 * Unit tests for {@link ProductService#updateArchiveStatus(String, String)}.
 * Uses a stub subclass of {@link ProductDao} to avoid requiring a live Elasticsearch instance.
 */
public class TestProductService {

    // ---------------------------------------------------------------------------
    // Stub ProductDao
    // ---------------------------------------------------------------------------

    /**
     * Minimal stub of ProductDao that allows test cases to control responses.
     */
    private static class StubProductDao extends ProductDao {

        /** Map: lidvid -> product class */
        private final java.util.Map<String, String> productClasses = new java.util.LinkedHashMap<>();
        /** Map: lid -> latest lidvid */
        private final java.util.Map<String, String> latestLidvids = new java.util.LinkedHashMap<>();
        /** Map: bundleLidvid -> LidvidSet of collection references */
        private final java.util.Map<String, LidvidSet> collectionIds = new java.util.LinkedHashMap<>();
        /** Map: collectionLidvid -> page count */
        private final java.util.Map<String, Integer> refDocCounts = new java.util.LinkedHashMap<>();

        /** Track which lidvids were updated and with what status */
        final List<String> updatedLidvids = new ArrayList<>();
        String lastStatus;

        StubProductDao() {
            // Pass nulls – the stub overrides all methods, so the real fields are unused.
            super(null, null);
        }

        void addProduct(String lidvid, String pClass) {
            productClasses.put(lidvid, pClass);
        }

        void addLatestLidvid(String lid, String lidvid) {
            latestLidvids.put(lid, lidvid);
        }

        void addCollectionIds(String bundleLidvid, Set<String> lids, Set<String> lidvids) {
            collectionIds.put(bundleLidvid, new LidvidSet(lids, lidvids));
        }

        void setRefDocCount(String collectionLidvid, int pages) {
            refDocCounts.put(collectionLidvid, pages);
        }

        @Override
        public String getProductClass(String lidvid) {
            return productClasses.get(lidvid);
        }

        @Override
        public List<String> getLatestLidVids(Collection<String> lids) {
            if (lids == null || lids.isEmpty()) return null;
            List<String> result = new ArrayList<>();
            for (String lid : lids) {
                String resolved = latestLidvids.get(lid);
                if (resolved != null) result.add(resolved);
            }
            return result.isEmpty() ? null : result;
        }

        @Override
        public LidvidSet getCollectionIds(String bundleLidvid) {
            return collectionIds.get(bundleLidvid);
        }

        @Override
        public int getRefDocCount(String collectionLidVid, char type) {
            return refDocCounts.getOrDefault(collectionLidVid, 0);
        }

        @Override
        public List<String> getRefs(String collectionLidVid, char type, int page) {
            // Return empty list; inventory test focus is on the count / status update tracking
            return Collections.emptyList();
        }

        @Override
        public void updateArchiveStatus(Collection<String> lidvids, String status) {
            if (lidvids == null) return;
            updatedLidvids.addAll(lidvids);
            lastStatus = status;
        }
    }

    // ---------------------------------------------------------------------------
    // Tests for bare LID resolution
    // ---------------------------------------------------------------------------

    /** Providing a bare LID that resolves to a known LIDVID should succeed. */
    @Test
    void testBareLidResolvesToLatestLidvid() throws Exception {
        StubProductDao dao = new StubProductDao();
        String lid = "urn:nasa:pds:my_bundle";
        String lidvid = "urn:nasa:pds:my_bundle::1.0";
        dao.addLatestLidvid(lid, lidvid);
        dao.addProduct(lidvid, "Product_Bundle");
        // No collections configured – cascade will warn but not fail
        dao.addCollectionIds(lidvid, Collections.emptySet(), Collections.emptySet());

        ProductService svc = new ProductService(dao);
        assertDoesNotThrow(() -> svc.updateArchiveStatus(lid, "archived"));

        // The bundle LIDVID should have been updated, not the bare LID
        assertEquals(List.of(lidvid), dao.updatedLidvids);
        assertEquals("archived", dao.lastStatus);
    }

    /** Providing a bare LID that cannot be resolved should throw a descriptive exception. */
    @Test
    void testBareLidNotFoundThrowsException() {
        StubProductDao dao = new StubProductDao();
        String lid = "urn:nasa:pds:nonexistent";

        ProductService svc = new ProductService(dao);
        Exception ex = assertThrows(Exception.class, () -> svc.updateArchiveStatus(lid, "archived"));
        // Message must mention the original identifier
        assertTrue(ex.getMessage().contains(lid), "Expected message to contain '" + lid + "': " + ex.getMessage());
    }

    /** Providing a LIDVID that does not exist should throw a descriptive exception. */
    @Test
    void testUnknownLidvidThrowsException() {
        StubProductDao dao = new StubProductDao();
        String lidvid = "urn:nasa:pds:my_bundle::99.0";

        ProductService svc = new ProductService(dao);
        Exception ex = assertThrows(Exception.class, () -> svc.updateArchiveStatus(lidvid, "archived"));
        assertTrue(ex.getMessage().contains(lidvid), "Expected message to contain '" + lidvid + "': " + ex.getMessage());
    }

    // ---------------------------------------------------------------------------
    // Tests for bundle cascade
    // ---------------------------------------------------------------------------

    /** Bundle cascade with empty collection references should not throw, but warn. */
    @Test
    void testBundleCascadeWithNoCollectionsDoesNotThrow() throws Exception {
        StubProductDao dao = new StubProductDao();
        String bundleLidvid = "urn:nasa:pds:my_bundle::1.0";
        dao.addProduct(bundleLidvid, "Product_Bundle");
        dao.addCollectionIds(bundleLidvid, Collections.emptySet(), Collections.emptySet());

        ProductService svc = new ProductService(dao);
        assertDoesNotThrow(() -> svc.updateArchiveStatus(bundleLidvid, "archived"));

        // Only the bundle itself should be updated
        assertEquals(List.of(bundleLidvid), dao.updatedLidvids);
    }

    /** Bundle cascade should update all collections referenced by LID. */
    @Test
    void testBundleCascadeWithCollectionLidUpdatesCollections() throws Exception {
        StubProductDao dao = new StubProductDao();
        String bundleLidvid = "urn:nasa:pds:my_bundle::1.0";
        String collectionLid = "urn:nasa:pds:my_collection";
        String collectionLidvid = "urn:nasa:pds:my_collection::1.0";

        dao.addProduct(bundleLidvid, "Product_Bundle");
        dao.addCollectionIds(bundleLidvid, new HashSet<>(Arrays.asList(collectionLid)), Collections.emptySet());
        dao.addLatestLidvid(collectionLid, collectionLidvid);
        dao.addProduct(collectionLidvid, "Product_Collection");
        // No primary products in inventory (count = 0, so just the collection itself)

        ProductService svc = new ProductService(dao);
        assertDoesNotThrow(() -> svc.updateArchiveStatus(bundleLidvid, "archived"));

        // Bundle and the resolved collection LIDVID must be updated
        assertTrue(dao.updatedLidvids.contains(bundleLidvid), "Bundle not updated");
        assertTrue(dao.updatedLidvids.contains(collectionLidvid), "Collection not updated");
    }

    /** Bundle cascade should update collections referenced by full LIDVID. */
    @Test
    void testBundleCascadeWithCollectionLidvidUpdatesCollection() throws Exception {
        StubProductDao dao = new StubProductDao();
        String bundleLidvid = "urn:nasa:pds:my_bundle::1.0";
        String collectionLidvid = "urn:nasa:pds:my_collection::2.0";

        dao.addProduct(bundleLidvid, "Product_Bundle");
        dao.addCollectionIds(bundleLidvid, Collections.emptySet(),
                new HashSet<>(Arrays.asList(collectionLidvid)));
        dao.addProduct(collectionLidvid, "Product_Collection");

        ProductService svc = new ProductService(dao);
        assertDoesNotThrow(() -> svc.updateArchiveStatus(bundleLidvid, "archived"));

        assertTrue(dao.updatedLidvids.contains(bundleLidvid), "Bundle not updated");
        assertTrue(dao.updatedLidvids.contains(collectionLidvid), "Collection not updated");
    }

    // ---------------------------------------------------------------------------
    // Tests for collection cascade
    // ---------------------------------------------------------------------------

    /** Collection update should cascade to primary products when inventory exists. */
    @Test
    void testCollectionCascadeWithInventory() throws Exception {
        StubProductDao dao = new StubProductDao() {
            @Override
            public List<String> getRefs(String collectionLidVid, char type, int page) {
                return Arrays.asList("urn:nasa:pds:my_collection:data:product1::1.0",
                        "urn:nasa:pds:my_collection:data:product2::1.0");
            }
        };
        String collectionLidvid = "urn:nasa:pds:my_collection::1.0";
        dao.addProduct(collectionLidvid, "Product_Collection");
        dao.setRefDocCount(collectionLidvid, 1);

        ProductService svc = new ProductService(dao);
        assertDoesNotThrow(() -> svc.updateArchiveStatus(collectionLidvid, "archived"));

        assertTrue(dao.updatedLidvids.contains(collectionLidvid), "Collection not updated");
        assertTrue(dao.updatedLidvids.contains("urn:nasa:pds:my_collection:data:product1::1.0"), "Product1 not updated");
        assertTrue(dao.updatedLidvids.contains("urn:nasa:pds:my_collection:data:product2::1.0"), "Product2 not updated");
    }
}
