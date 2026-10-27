package gov.nasa.pds.harvest.util.out;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * Singleton to hold references to registry metadata, collection inventory (product references) and
 * other data writers.
 * 
 * @author karpenko
 */
public class WriterManager {
  public final File outdir;
  private static WriterManager instance;
  private static final Logger log = LogManager.getLogger(WriterManager.class);

  private SupplementalWriter supWriter;


  /**
   * Provate constructor. Use getInstance() instead.
   */
  private WriterManager(File outDir) throws Exception {
    outdir = outDir;
    supWriter = new SupplementalWriter(outDir);
  }


  /**
   * Create writers to write JSON data files with metadata extracted from PDS4 labels. Generated
   * JSON files can be imported into Elasticsearch by Registry manager tool.
   * 
   * @param outDir output directory
   * @throws Exception an exception
   */
  public static void init(File outDir) throws Exception {
    if (instance != null)
      throw new Exception("WriterManager is already initialized.");
    instance = new WriterManager(outDir);
  }


  /**
   * Clean up resources / close files.
   */
  public static void destroy() {
    WriterManager.destroy(false);
  }
  
  public static void destroy(boolean cleanup) {
    if (instance == null)
      return;

    if (cleanup) {
      try (var stream = Files.walk(getInstance().outdir.toPath())) {
        stream.sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
      } catch (Exception e) {
        log.error("Could not cleanup harvest mess. Later tasks might fail in a mysterious way.", e);
        throw new IllegalStateException("Should never got here as should be able to clean up after ones self.");
      }
    }

    CloseUtils.close(instance.supWriter);
    instance = null;
  }


  /**
   * Get singleton instance.
   * 
   * @return the singleton instance.
   * @throws Exception an exception.
   */
  public static WriterManager getInstance() throws Exception {
    if (instance == null)
      throw new Exception("WriterManager is not initialized.");

    return instance;
  }


  /**
   * Get a writer for supplemental products
   * 
   * @return a writer for supplemental products
   */
  public SupplementalWriter getSupplementalWriter() {
    return supWriter;
  }
}
