package gov.nasa.pds.registry.common.dd;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.dd.parser.DDAttribute;
import gov.nasa.pds.registry.common.es.dao.dd.DataTypeNotFoundException;


/**
 * Writes Elasticsearch JSON data file to be loaded into data dictionary index.
 * 
 * @author karpenko
 */
public class LddEsJsonWriter {
  private Logger log;
  private DDNJsonWriter writer;
  private DDRecord ddRec = new DDRecord();

  private Pds2EsDataTypeMap dtMap;
  private Map<String, DDAttribute> ddAttrCache;
  private String nsFilter;


  /**
   * Constructor
   * 
   * @param outFile Elasticsearch JSON data file
   * @param dtMap PDS to Elasticsearch data type map
   * @param ddAttrCache LDD attribute cache
   * @throws Exception an exception
   */
  public LddEsJsonWriter(File outFile, Pds2EsDataTypeMap dtMap,
      Map<String, DDAttribute> ddAttrCache, boolean overwrite) {
    log = LogManager.getLogger(this.getClass());
    writer = new DDNJsonWriter(outFile, overwrite);
    this.dtMap = dtMap;
    this.ddAttrCache = ddAttrCache;
  }


  /**
   * Set namespace filter. Only process classes having this namespace.
   * 
   * @param filter namespace, such as 'pds'
   */
  public void setNamespaceFilter(String filter) {
    this.nsFilter = filter;
  }

  /**
   * Write field definition (Elasticsearch field name, data type and other information)
   * 
   * @param classNs LDD class namespace
   * @param className LDD class name
   * @param attrId LDD attribute ID
   * @throws IOException 
   * @throws Exception an exception
   */
  public void writeFieldDefinition(String classNs, String className, String attrId) throws IOException {
    // Apply namespace filter
    if (nsFilter != null && !nsFilter.equals(classNs))
      return;

    DDAttribute attr = ddAttrCache.get(attrId);
    if (attr == null) {
      log.warn("Missing attribute " + attrId);
    } else {
      writeRecord(classNs, className, attr);
    }
  }


  /**
   * Write PDS LDD version and date
   * 
   * @param namespace namespace
   * @param schemaFileName schema file name
   * @param imVersion IM version
   * @param lddVersion LDD version
   * @param date date
   * @throws IOException 
   * @throws ParseException 
   * @throws Exception an exception
   */
  public void writeLddInfo(String namespace, String schemaFileName, String imVersion,
      String lddVersion, String date) throws IOException, ParseException {
    if (namespace == null || namespace.isBlank())
      throw new IllegalArgumentException("Missing data dictionary namespace");
    if (date == null || date.isBlank())
      throw new IllegalArgumentException("Missing data dictionary date");

    DDRecord rec = new DDRecord();
    rec.classNs = "registry";
    rec.className = "LDD_Info";
    rec.attrNs = namespace;
    rec.attrName = schemaFileName;

    rec.imVersion = imVersion;
    rec.lddVersion = lddVersion;
    rec.date = LddUtils.lddDateToIsoInstantString(date);

    // Overwrite existing record
    writer.write(rec.esFieldNameFromComponents(), rec, "index");
  }


  private void writeRecord(String classNs, String className, DDAttribute dda) throws IOException {
    // Assign values
    ddRec.classNs = classNs;
    ddRec.className = className;
    ddRec.attrNs = dda.attrNs;
    ddRec.attrName = dda.attrName;

    ddRec.dataType = dda.dataType;
    try {
      ddRec.esDataType = dtMap.getEsDataType(dda.dataType);
      ddRec.description = dda.description;
      writer.write(ddRec.esFieldNameFromComponents(), ddRec);

      // Fix wrong attribute namespace
      if (!classNs.equals(dda.attrNs)) {
        ddRec.attrNs = classNs;
        writer.write(ddRec.esFieldNameFromComponents(), ddRec);
      }
    } catch (DataTypeNotFoundException e) {
      log.error("The field '{}' will not be searchable because LDD does not contain a type for it (see harvest#204).", dda.attrName);
    }
  }

}
