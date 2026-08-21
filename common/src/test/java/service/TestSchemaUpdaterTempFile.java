package service;

import gov.nasa.pds.registry.common.es.service.SchemaUpdater;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class TestSchemaUpdaterTempFile {

    @Test
    void createLddTempFile_createsWritableFile() throws Exception {
        File tmp = SchemaUpdater.createLddTempFile("test");
        try {
            assertTrue(tmp.exists(), "Temp file must exist");
            assertTrue(tmp.canWrite(), "Temp file must be writable");
            assertTrue(tmp.getName().startsWith("LDD-"), "Temp file must have LDD- prefix");
            assertTrue(tmp.getName().endsWith(".JSON"), "Temp file must have .JSON suffix");
        } finally {
            tmp.delete();
        }
    }

    @Test
    void createLddTempFile_recreatesMissingTmpDir() throws Exception {
        String originalTmpDir = System.getProperty("java.io.tmpdir");
        File missingTmpDir = new File(System.getProperty("java.io.tmpdir"),
            "ldd-tmp-test-" + System.nanoTime());
        assertFalse(missingTmpDir.exists(), "Precondition: test tmp dir must not already exist");

        System.setProperty("java.io.tmpdir", missingTmpDir.getAbsolutePath());
        File tmp = null;
        try {
            tmp = SchemaUpdater.createLddTempFile("test");
            assertTrue(missingTmpDir.exists(), "Missing java.io.tmpdir must be created");
            assertTrue(tmp.exists(), "Temp file must exist");
        } finally {
            System.setProperty("java.io.tmpdir", originalTmpDir);
            if (tmp != null) {
                tmp.delete();
            }
            missingTmpDir.delete();
        }
    }
}
