package gov.nasa.pds.registry.common.es.service;

import gov.nasa.pds.registry.common.dd.LddException;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TestSearchIndexWait {

    private static final Logger log = LogManager.getLogger(TestSearchIndexWait.class);

    // --- untilReady ---

    @Test
    void untilReady_returnsImmediatelyWhenAlreadyReady() throws Exception {
        String result = SearchIndexWait.untilReady(5, () -> "hello", s -> !s.isEmpty(), log, "test");
        assertEquals("hello", result);
    }

    @Test
    void untilReady_retriesAndSucceedsOnSecondCall() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        String result = SearchIndexWait.untilReady(2,
            () -> calls.incrementAndGet() >= 2 ? "ready" : "",
            s -> !s.isEmpty(), log, "test");
        assertEquals("ready", result);
        assertEquals(2, calls.get());
    }

    @Test
    void untilReady_returnsLastResultOnTimeout() throws Exception {
        // Condition never becomes true; should return empty string after 1 attempt (maxSeconds=0)
        String result = SearchIndexWait.untilReady(0, () -> "", s -> !s.isEmpty(), log, "test");
        assertEquals("", result);
    }

    @Test
    void untilReady_propagatesNonInterruptException() {
        assertThrows(IOException.class, () ->
            SearchIndexWait.untilReady(5, () -> { throw new IOException("fail"); }, s -> true, log, "test"));
    }

    // --- untilVisible ---

    @Test
    void untilVisible_returnsImmediatelyWhenOpSucceeds() throws Exception {
        String result = SearchIndexWait.untilVisible(5, () -> "visible", log, "test");
        assertEquals("visible", result);
    }

    @Test
    void untilVisible_retriesAndSucceedsOnSecondCall() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        String result = SearchIndexWait.untilVisible(2, () -> {
            if (calls.incrementAndGet() < 2) throw new DataTypeNotFoundException("f");
            return "visible";
        }, log, "test");
        assertEquals("visible", result);
        assertEquals(2, calls.get());
    }

    @Test
    void untilVisible_throwsDataTypeNotFoundExceptionOnTimeout() {
        DataTypeNotFoundException ex = assertThrows(DataTypeNotFoundException.class, () ->
            SearchIndexWait.untilVisible(0,
                () -> { throw new DataTypeNotFoundException("missing"); },
                log, "test"));
        assertTrue(ex.getMissingFields().contains("missing"));
    }

    @Test
    void untilVisible_propagatesNonRetryableExceptionImmediately() {
        AtomicInteger calls = new AtomicInteger(0);
        assertThrows(IOException.class, () ->
            SearchIndexWait.untilVisible(30, () -> {
                calls.incrementAndGet();
                throw new IOException("network failure");
            }, log, "test"));
        assertEquals(1, calls.get());
    }

    // --- interrupt handling ---

    @Test
    void untilReady_throwsLddExceptionOnInterrupt() throws Exception {
        Thread t = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                SearchIndexWait.untilReady(30, () -> "", s -> !s.isEmpty(), log, "test");
                fail("Expected LddException");
            } catch (LddException e) {
                assertTrue(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }
        });
        t.start();
        t.join(5000);
        assertFalse(t.isAlive());
    }

    @Test
    void untilVisible_throwsLddExceptionOnInterrupt() throws Exception {
        Thread t = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                SearchIndexWait.untilVisible(30,
                    () -> { throw new DataTypeNotFoundException("f"); }, log, "test");
                fail("Expected LddException");
            } catch (LddException e) {
                assertTrue(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
                fail("Unexpected exception: " + e);
            }
        });
        t.start();
        t.join(5000);
        assertFalse(t.isAlive());
    }
}
