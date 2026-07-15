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
}
