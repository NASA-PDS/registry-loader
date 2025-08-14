package gov.nasa.pds.registry.common.meta;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.w3c.dom.Document;
import gov.nasa.pds.registry.common.util.json.Serializer;
import gov.nasa.pds.registry.common.util.xml.TransformToCollection;
import gov.nasa.pds.registry.common.util.xml.XmlNamespaces;


/**
 * Metadata extracted from PDS label.
 * 
 * @author karpenko
 */
public class Metadata {
  private final HashMap<String, Object> document = new HashMap<String, Object>();
  private final HashMap<String, Object> tracking = new HashMap<String, Object>();
  private final String FLD_ALTERNATE_IDS = "alternate_ids";
  private final String FLD_NODE_NAME = "ops:node_name";
  private final String FLD_HARVEST_DATE_TIME = "ops:harvest_date_time";
  private final String FLD_HARVEST_VERSION = "ops:harvest_version";
  public static final String FLD_ARCHIVE_STATUS = "ops:archive_status";
  private final Set<String> dataFiles; // File names from <File_Area...> tags
  private Set<String> fieldNames = new HashSet<String>();
  private XmlNamespaces xmlns = new XmlNamespaces();

  /**
   * Constructor
   */
  Metadata(String lid, String vid, String prodClass, String title, Set<String> dataFiles) {
    ArrayList<String> altids = new ArrayList<String>();
    this.dataFiles = dataFiles == null ? new HashSet<String>() : dataFiles;
    this.document.put("ops:Tracking_Meta", tracking);
    this.document.put(FLD_ALTERNATE_IDS, altids);
    this.document.put("lid", lid);
    this.document.put("vid", vid);
    this.document.put("lidvid", lid + "::" + vid);
    this.document.put("title", title);
    this.document.put("product_class", prodClass);
    altids.add(this.lid());
    altids.add(this.lidvid());
  }

  /**
   * Set node name
   * 
   * @param name node name
   */
  public void setNodeName(String name) {
    this.tracking.put(FLD_NODE_NAME, name);
  }

  /**
   * Set harvest timestamp
   * 
   * @param val timestamp
   */
  public void setHarvestTimestamp(Instant val) {
    String strVal = DateTimeFormatter.ISO_INSTANT.format(val);
    this.tracking.put(FLD_HARVEST_DATE_TIME, strVal);
  }

  /**
   * Set harvest version
   * 
   * @param val version
   */
  public void setHarvestVersion(String val) {
    this.tracking.put(FLD_HARVEST_VERSION, val);
  }

  /**
   * Set archive status
   * 
   * @param status archive status
   */
  public void setArchiveStatus(String status) {
    this.tracking.put(FLD_ARCHIVE_STATUS, status);
  }

  public void setProduct(Document product, MetadataNormalizer normalizer) {
    TransformToCollection transformer = new TransformToCollection();
    this.document.putAll(transformer.convert(product, normalizer));
    this.fieldNames = transformer.leavesFound();
    this.xmlns = transformer.xmlnsInfo();
  }
  /**
   * terrible hack to set the harvest version from something that depends on this module.
   */
  private static String harvest_version = "not set by harvest";

  public static String getReportedHarvestVersion() {
    return Metadata.harvest_version;
  }

  public static void reportHarvestVersion(String value) {
    if (value != null) {
      Metadata.harvest_version = value;
    }
  }

  public Serializer.Pair asBulkPair(String jobId) {
    HashMap<String, HashMap<String,String>> command =
        new HashMap<String, HashMap<String,String>>();
    command.put("index", new HashMap<String,String>());
    command.get("index").put("_id", this.lidvid());
    this.document.put("_package_id", jobId);
    return new Serializer(false).new Pair(command, this.document);
  }

  public String lid() {
    return this.document.get("lid").toString();
  }

  public String lidvid() {
    return this.document.get("lidvid").toString();
  }
  
  public String vid() {
    return this.document.get("vid").toString();
  }
  public void setLabelInfo (Object labelMetadata) {
    this.document.put("ops:Label_File_Info", labelMetadata);
  }
  
  public Set<String> dataFiles() {
    return new HashSet<String>(this.dataFiles);
  }
  
  public void setDataFileInfo (Object datafileMetadata) {
    this.document.put("ops:Data_File_Info", datafileMetadata);
  }
  public Set<String> fieldnames() {
    return this.fieldNames;
  }
  public XmlNamespaces xmlns() {
    return this.xmlns;
  }
  public void updateInternalReferences (Map<String,Set<String>> refs) {
    this.document.putAll(refs);
  }
}
