package gov.nasa.pds.registry.common.meta;

import java.io.File;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import gov.nasa.pds.registry.common.util.xml.XPathUtils;


/**
 * Extract basic metadata, such as LID, VID, title from a PDS label.
 * 
 * @author karpenko
 */
public class BasicMetadataExtractor {
  final public static String DEFAULT_ARCHIVE_STATUS = "staged";
  private final XPathExpression xLid;
  private final XPathExpression xVid;
  private final XPathExpression xTitle;
  private final XPathExpression xFileName;
  private final XPathExpression xDocFile;

  /**
   * Constructor
   * 
   * @throws Exception an exception
   */
  public BasicMetadataExtractor() throws Exception {
    XPathFactory xpf = XPathFactory.newInstance();

    this.xLid = XPathUtils.compileXPath(xpf, "//*[local-name() = 'Identification_Area']/*[local-name() = 'logical_identifier']");
    this.xVid = XPathUtils.compileXPath(xpf, "//*[local-name() = 'Identification_Area']/*[local-name() = 'version_id']");
    this.xTitle = XPathUtils.compileXPath(xpf, "//*[local-name() = 'Identification_Area']/*[local-name() = 'title']");
    this.xFileName = XPathUtils.compileXPath(xpf, "//*[local-name() = 'File']/*[local-name() = 'file_name']");
    this.xDocFile = XPathUtils.compileXPath(xpf, "//*[local-name() = 'Document_File']");
  }


  /**
   * Extract basic metadata from a PDS label
   * 
   * @param file PDS label file
   * @param doc Parsed PDS label file (XML DOM)
   * @return extracted metadata
   * @throws Exception an exception
   */
  public Metadata extract(File file, Document doc, String status) throws Exception {
    // Product class
    String prodClass = doc.getDocumentElement().getNodeName();

    // LID
    String lid = trim(XPathUtils.getStringValue(doc, xLid));
    if (lid == null || lid.isEmpty()) {
      throw new Exception("Missing logical identifier: " + file);
    }

    // VID
    String vid = trim(XPathUtils.getStringValue(doc, xVid));
    if (vid == null || vid.isEmpty()) {
      throw new Exception("Missing '//Identification_Area/version_id'");
    }

    // Title
    String title = StringUtils.normalizeSpace(XPathUtils.getStringValue(doc, xTitle));

    // Files
    Set<String> dataFiles;
    if (prodClass.equals("Product_Document")) {
      dataFiles = extractDocumentFilePaths(doc);
    } else {
      dataFiles = XPathUtils.getStringSet(doc, xFileName);
    }

    Metadata md = new Metadata(lid,vid,prodClass,title,dataFiles);
    // Set default fields
    md.setHarvestTimestamp(Instant.now());
    md.setArchiveStatus(status);
    md.setHarvestVersion(Metadata.getReportedHarvestVersion());
    return md;
  }


  private Set<String> extractDocumentFilePaths(Document doc) throws Exception {
    NodeList nodes = XPathUtils.getNodeList(doc, xDocFile);
    Set<String> files = new TreeSet<>();

    if (nodes == null) return files;

    for (int i = 0; i < nodes.getLength(); i++) {
      String filePath = extractFilePath(nodes.item(i));
      if (filePath != null)
        files.add(filePath);
    }

    return files;
  }


  private String extractFilePath(Node root) {
    String fileName = null;
    String dir = null;

    NodeList nodes = root.getChildNodes();

    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String nodeName = node.getNodeName();

      if (nodeName.equals("file_name")) {
        fileName = node.getTextContent().trim();
      } else if (nodeName.equals("directory_path_name")) {
        dir = node.getTextContent().trim();
      }
    }

    if (fileName == null)
      return null;

    if (dir == null)
      return fileName;

    return dir.endsWith("/") ? dir + fileName : dir + "/" + fileName;
  }


  private static String trim(String str) {
    if (str == null)
      return null;
    str = str.trim();

    return str.isEmpty() ? null : str;
  }

}
