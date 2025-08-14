package gov.nasa.pds.registry.common.util.xml;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import gov.nasa.pds.registry.common.meta.MetaConstants;
import gov.nasa.pds.registry.common.meta.MetadataNormalizer;

public class TransformToCollection {
  private final HashSet<String> leaves = new HashSet<String>();
  private MetadataNormalizer normalizer;
  private XmlNamespaces xmlnsInfo;

  public Set<String> leavesFound() {
    return new HashSet<String>(this.leaves);
  }
  public XmlNamespaces xmlnsInfo() {
    return this.xmlnsInfo;
  }
 
  @SuppressWarnings("unchecked")
  public Map<String,Object> convert (Document doc, MetadataNormalizer normalizer) {
    Element root = doc.getDocumentElement();
    //HashMap<String,Object> label = new HashMap<String,Object>();
    this.normalizer = normalizer;
    this.xmlnsInfo = NsUtils.getNamespaces(doc);
    //label.put("label", this.convert(root, "", false));
    //return label;
    return (Map<String,Object>)convert (root, "");
  }
  private Object convert (Node parent, String heritage) {
    HashMap<String,Object> content = new HashMap<String,Object>();
    if (!heritage.isBlank() && parent.hasAttributes()) {
      NamedNodeMap attrs = parent.getAttributes();
      for (int i = 0 ; i < attrs.getLength() ; i++) {
        String name = "@" + attrs.item(i).getNodeName();
        content.put(name, attrs.item(i).getNodeValue());
        this.leaves.add(heritage + name);
      }
    }
    if (parent.hasChildNodes()) {
      NodeList children = parent.getChildNodes();
      if (this.isLeaf(children)) {
        this.leaves.add(heritage);
        return this.normalizer.normalizeValue(heritage, parent.getTextContent());       
      }
      for (int i = 0 ; i < children.getLength() ; i++) {
        Node child = children.item(i);
        if (child.getNodeType() == Node.ELEMENT_NODE) {
          String name = this.getNsName(child);
          content.put(name, this.convert(child, heritage + (heritage.isBlank() ? "":".") + name));
        }
      }
    }
    if (content.size() == 0) {
      throw new NoSuchElementException(heritage + " is empty which should not be happening.");
    }
    return content;
  }
  private boolean isLeaf (NodeList children) {
    boolean result = true;
    for (int i = 0 ; result && i < children.getLength() ; i++) {
      int type = children.item(i).getNodeType();
      result &= (type == Node.TEXT_NODE || type == Node.COMMENT_NODE);
    }
    return result;
  }
  private String getNsName(Node node) {
    if (!this.xmlnsInfo.uri2prefix.containsKey(node.getNamespaceURI())) {
      throw new NoSuchElementException("Unknown namespace: " + node.getNamespaceURI());
    }
    String nsUri = node.getNamespaceURI();
    String nsPrefix = this.xmlnsInfo.uri2prefix.get(nsUri);
    String nsName = nsPrefix + MetaConstants.NS_SEPARATOR + node.getLocalName();
    return nsName;
  }
}
