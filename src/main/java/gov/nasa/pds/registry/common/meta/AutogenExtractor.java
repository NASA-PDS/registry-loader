package gov.nasa.pds.registry.common.meta;

import java.io.File;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import gov.nasa.pds.registry.common.util.FieldMap;
import gov.nasa.pds.registry.common.util.date.PdsDateConverter;
import gov.nasa.pds.registry.common.util.xml.NsUtils;
import gov.nasa.pds.registry.common.util.xml.XmlDomUtils;
import gov.nasa.pds.registry.common.util.xml.XmlNamespaces;


/**
 * Generates key-value pairs for all fields in a PDS label.
 * @author karpenko
 */
public class AutogenExtractor
{
    private Set<String> classFilterIncludes;
    private Set<String> classFilterExcludes;
    private Set<String> dateFields;
    
    private XmlNamespaces xmlnsInfo;
    private FieldMap fields;
    private PdsDateConverter dateConverter;
    
   
    /**
     * Constructor
     */
    public AutogenExtractor()
    {
        dateConverter = new PdsDateConverter(false);
    }

    
    /**
     * Set class filters. 
     * NOTE: You could not have both include and exclude filters at the same time.
     * @param include include classes
     * @param exclude exclude classes
     */
    public void setClassFilters(Set<String> include, Set<String> exclude)
    {
        this.classFilterIncludes = include;
        this.classFilterExcludes = exclude;
    }
    
    
    /**
     * Set date fields. Will try to convert these fields to ISO format.
     * @param fields date field names
     */
    public void setDateFields(Set<String> fields)
    {
        this.dateFields = fields;
    }

    
    /**
     * Extracts all fields from a label file into a FieldMap
     * @param file PDS label file
     * @param fields key-value pairs (output parameter)
     * @return XML namespace mappings
     * @throws Exception an exception
     */
    public XmlNamespaces extract(File file, FieldMap fields) throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        Document doc = XmlDomUtils.readXml(dbf, file);
        
        this.xmlnsInfo = NsUtils.getNamespaces(doc);
        this.fields = fields;

        Element root = doc.getDocumentElement();
        processNode(root);
        
        return this.xmlnsInfo;
    }
    
    
    private void processNode(Node node) throws Exception
    {
        boolean isLeaf = true;
        
        NodeList nl = node.getChildNodes();
        for(int i = 0; i < nl.getLength(); i++)
        {
            Node cn = nl.item(i);
            if(cn.getNodeType() == Node.ELEMENT_NODE)
            {
                isLeaf = false;
                // Process children recursively
                processNode(cn);
            }
        }
        
        // This is a leaf node. Get value.
        if(isLeaf)
        {
            processLeafNode(node);
        }
    }

    
    private void processLeafNode(Node node) throws Exception
    {
        // Data dictionary class and attribute
        String className = getNsName(node.getParentNode());
        
        // Apply class filters
        if(classFilterIncludes != null && classFilterIncludes.size() > 0)
        {
            if(!classFilterIncludes.contains(className)) return;
        }
        if(classFilterExcludes != null && classFilterExcludes.size() > 0)
        {
            if(classFilterExcludes.contains(className)) return;
        }
        
        String attrName = getNsName(node);
        String fieldName = className + MetaConstants.ATTR_SEPARATOR + attrName;
        
        // Field value
        String fieldValue = StringUtils.normalizeSpace(node.getTextContent());
        
        // Convert dates to "ISO instant" format
        String nodeName = node.getLocalName();
        if(nodeName.contains("date") || dateFields.contains(fieldName))
        {
            fieldValue = dateConverter.toIsoInstantString(nodeName, fieldValue);
        }
        
        fields.addValue(fieldName, fieldValue);
    }
    
    
    private String getNsName(Node node) throws Exception
    {
        String nsUri = node.getNamespaceURI();
        String nsPrefix = xmlnsInfo.uri2prefix.get(nsUri);
        if(nsPrefix == null) 
        {
            throw new Exception("Unknown namespace: " + nsUri);    
        }
        
        String nsName = nsPrefix + MetaConstants.NS_SEPARATOR + node.getLocalName();
        
        return nsName;
    }
    
}
