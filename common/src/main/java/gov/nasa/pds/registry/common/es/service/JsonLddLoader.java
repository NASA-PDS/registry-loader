package gov.nasa.pds.registry.common.es.service;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.ConnectionFactory;
import gov.nasa.pds.registry.common.dd.LddEsJsonWriter;
import gov.nasa.pds.registry.common.dd.LddUtils;
import gov.nasa.pds.registry.common.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.common.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.common.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.registry.common.dd.parser.DDAttribute;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.dao.dd.LddVersions;


/**
 * Loads PDS LDD JSON file into Elasticsearch data dictionary index
 *
 * @author karpenko
 */
public class JsonLddLoader {
  private Logger log;

  private Pds2EsDataTypeMap dtMap;
  private DataLoader loader;

  private DataDictionaryDao dao;
  private final Set<String> loadedThisRun = ConcurrentHashMap.newKeySet();

  /**
   * Constructor
   *
   * @param dao Data dictionary data access object
   * @param conFact instance of class gov.nasa.pds.registry.common.ConnectionFactory
   * @throws Exception an exception
   */
  public JsonLddLoader(DataDictionaryDao dao, ConnectionFactory conFact) throws Exception {
    log = LogManager.getLogger(this.getClass());
    dtMap = new Pds2EsDataTypeMap();

    loader = new DataLoader(conFact.clone().setIndexName(conFact.getIndexName() + "-dd"));
    this.dao = dao;
  }


  /**
   * Load PDS to Elasticsearch data type map
   *
   * @param file configuration file
   * @throws Exception an exception
   */
  public void loadPds2EsDataTypeMap(File file) throws Exception {
    dtMap.load(file);
  }


  /**
   * Load PDS LDD JSON file into Elasticsearch data dictionary index
   *
   * @param lddFile PDS LDD JSON file
   * @param namespace Namespace filter. Only load classes having this namespace.
   * @throws Exception an exception
   */
  public void load(File lddFile, String namespace) throws Exception {
    String lddFileName = lddFile.getName();
    load(lddFile, lddFileName, namespace);
  }


  /**
   * Load PDS LDD JSON file into Elasticsearch data dictionary index
   *
   * @param lddFile PDS LDD JSON file
   * @param lddFileName file name to store in Elasticsearch (could be different from lddFile).
   *        lddFile could point to a temporary file loaded from the Internet.
   * @param namespace Namespace filter. Only load classes having this namespace.
   * @throws Exception an exception
   */
  public void load(File lddFile, String lddFileName, String namespace) throws Exception {
    // If a namespace is not provided get it from the LDD.
    // If there are more than one namespace, an exception will be thrown.
    if (namespace == null || namespace.isBlank()) {
      namespace = LddUtils.getLddNamespace(lddFile);
    }

    // Key combines namespace and filename so two namespaces using the same filename
    // don't incorrectly share a cache entry.
    String cacheKey = namespace + ":" + lddFileName;

    // Short-circuit: if we already loaded this LDD file in this JVM run, skip the
    // AOSS query entirely. This prevents re-downloads when the LDD_Info sentinel is
    // not yet visible via search immediately after a bulk load (AOSS propagation lag).
    //
    // Note: contains() + add() below is not atomic, so two threads racing on the same
    // key can both proceed to loadOnly(). That is safe because loadOnly() is idempotent
    // (AOSS bulk loads with the same document IDs overwrite with identical data), so the
    // only cost is redundant network work. Coarse synchronization would block all threads
    // on a per-LDD AOSS wait (~30s), which is worse.
    if (loadedThisRun.contains(cacheKey)) {
      log.debug("LDD {} already loaded in this run, skipping.", lddFileName);
      return;
    }

    // Get information about LDDs already loaded into the registry (for this namespace)
    LddVersions info = dao.getLddInfo(namespace);
    if (info.files.contains(lddFileName)) {
      log.debug("LDD {} already loaded in registry.", lddFileName);
      loadedThisRun.add(cacheKey);
      return;
    }

    // Create and load temporary data file into Elasticsearch
    boolean visible = loadOnly(lddFile, lddFileName, namespace, info.lastDate);
    // Only cache as loaded when the LDD became fully visible; if loadOnly timed out
    // with a warn-and-continue, the next call will still check AOSS.
    if (visible) {
      loadedThisRun.add(cacheKey);
    }
  }


  /**
   * Load PDS LDD JSON file into Elasticsearch data dictionary index. Do not validate parameters.
   * This is a low level method called by other classes / methods.
   *
   * @param lddFile PDS LDD JSON file
   * @param lddFileName file name to store in Elasticsearch (could be different from lddFile).
   *        lddFile could point to a temporary file loaded from the Internet.
   * @param namespace Namespace filter. Only load classes having this namespace.
   * @param lastDate last date of an LDD for given namespace already loaded into registry
   * @return true if the LDD was fully loaded and became visible; false if a timeout or zero-field skip occurred
   * @throws Exception an exception
   */
  boolean loadOnly(File lddFile, String lddFileName, String namespace, Instant lastDate)
      throws Exception {
    // Create and load temporary data file into Elasticsearch
    File tempEsDataFile = File.createTempFile("es-", ".json");
    log.debug("Creating temporary ES data file " + tempEsDataFile.getAbsolutePath());

    try {
      String firstFieldId = createEsDataFile(lddFile, lddFileName, namespace, tempEsDataFile, lastDate);
      if (firstFieldId == null) {
        // createEsDataFile logged a warning; nothing to load.
        return false;
      }
      loader.loadFile(tempEsDataFile);

      // Wait until the newly loaded LDD file is visible in the LDD_Info sentinel (confirms
      // bulk load completed). Checking v.files.contains(lddFileName) rather than !v.isEmpty()
      // prevents a false pass when prior LDDs for the same namespace are already indexed.
      LddVersions info = SearchIndexWait.untilReady(SearchIndexWait.DEFAULT_WAIT_SECONDS,
          () -> { try { return dao.getLddInfoNoCache(namespace); } catch (IOException e) { throw e; } catch (Exception e) { throw new IOException(e); } },
          v -> v.files.contains(lddFileName), log, "LDD sentinel for namespace " + namespace);
      if (info.isEmpty() || !info.files.contains(lddFileName)) {
        log.warn("LDD {} not indexed after {} seconds. It may be indexed later, but there may be a delay in loading other LDDs for this namespace.",
            namespace, SearchIndexWait.DEFAULT_WAIT_SECONDS);
        return false;
      }
      log.debug("LDD {} indexed with date {}. Waiting for mget visibility.", namespace, info.lastDate);

      // On AOSS, mget and search use different visibility paths. Wait until the first field
      // document is also reachable via mget so that getDataTypes() calls succeed immediately.
      try {
        SearchIndexWait.untilVisible(SearchIndexWait.DEFAULT_WAIT_SECONDS,
            () -> dao.getDataTypes(Collections.singletonList(firstFieldId), true),
            log, "field " + firstFieldId + " of namespace " + namespace + " via mget");
      } catch (DataTypeNotFoundException e) {
        log.warn("Field {} of namespace {} not visible via mget after {} seconds. Schema update may retry.",
            firstFieldId, namespace, SearchIndexWait.DEFAULT_WAIT_SECONDS);
        return false;
      }
      log.debug("Visibility of namespace {} fully validated.", namespace);
      return true;
    } finally {
      // Delete temporary file
      tempEsDataFile.delete();
    }
  }


  private static class CaaCallback implements ClassAttrAssociationParser.Callback {
    private final LddEsJsonWriter writer;
    private final String namespace;
    private final Map<String, DDAttribute> ddAttrCache;
    private String firstFieldId;

    public CaaCallback(LddEsJsonWriter writer, String namespace, Map<String, DDAttribute> ddAttrCache) {
      this.writer = writer;
      this.namespace = namespace;
      this.ddAttrCache = ddAttrCache;
    }

    @Override
    public void onAssociation(String classNs, String className, String attrId) throws Exception {
      writer.writeFieldDefinition(classNs, className, attrId);
      if (firstFieldId == null && namespace.equals(classNs)) {
        DDAttribute attr = ddAttrCache.get(attrId);
        if (attr != null) {
          firstFieldId = classNs + ":" + className + "/" + attr.attrNs + ":" + attr.attrName;
        }
      }
    }

    public String getFirstFieldId() {
      return firstFieldId;
    }
  }


  /**
   * Create Elasticsearch data file to be loaded into data dictionary index.
   * Returns the first field ID written (for use in mget visibility checks), or null if zero fields
   * were produced for the requested namespace (in which case the LDD_Info sentinel is NOT written).
   *
   * @param lddFile PDS LDD JSON file
   * @param namespace Namespace filter. Only load classes having this namespace.
   * @param tempEsFile Write to this Elasticsearch file
   * @return first field ID, or null if zero fields were produced
   * @throws Exception an exception
   */
  private String createEsDataFile(File lddFile, String lddFileName, String namespace, File tempEsFile,
      Instant lastDate) throws Exception {
    // Parse and cache LDD attributes
    Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
    AttributeDictionaryParser.Callback acb = (attr) -> {
      ddAttrCache.put(attr.id, attr);
    };
    AttributeDictionaryParser attrParser = new AttributeDictionaryParser(lddFile, acb);
    attrParser.parse();

    // If this LDD date is after the last stored in Elasticsearch, overwrite old records
    boolean overwrite = overwriteLdd(lastDate, attrParser.getLddDate());

    // Create a writer to save LDD data in Elasticsearch JSON data file
    LddEsJsonWriter writer = new LddEsJsonWriter(tempEsFile, dtMap, ddAttrCache, overwrite);
    writer.setNamespaceFilter(namespace);

    // Parse class attribute associations and write to ES data file
    CaaCallback ccb = new CaaCallback(writer, namespace, ddAttrCache);
    ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(lddFile, ccb);
    caaParser.parse();

    String firstFieldId = ccb.getFirstFieldId();
    if (firstFieldId == null) {
      // Zero fields were written for this namespace — do not write the LDD_Info sentinel.
      // A sentinel with no field documents would cause all future runs to skip this LDD
      // (believing it already loaded), leaving the namespace permanently empty in -dd.
      log.warn("LDD {} produced no field documents for namespace '{}'. "
          + "The LDD will be re-attempted on the next run. "
          + "If this persists, the LDD JSON may be malformed or use an unrecognised format.",
          lddFileName, namespace);
      return null;
    }

    // Write data dictionary version and date
    writer.writeLddInfo(namespace, lddFileName, attrParser.getImVersion(),
        attrParser.getLddVersion(), attrParser.getLddDate());

    return firstFieldId;
  }


  private boolean overwriteLdd(Instant lastDate, String strLddDate) {
    try {
      Instant lddDate = LddUtils.lddDateToIsoInstant(strLddDate);
      return lddDate.isAfter(lastDate);
    } catch (Exception ex) {
      log.warn("Could not parse LDD date " + strLddDate);
      return false;
    }
  }

}
