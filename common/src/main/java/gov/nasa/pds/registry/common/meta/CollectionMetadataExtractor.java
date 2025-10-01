package gov.nasa.pds.registry.common.meta;

import java.util.Set;

import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;

import gov.nasa.pds.registry.common.util.xml.XPathUtils;


/**
 * Extracts collection metadata
 * @author karpenko
 */
public class CollectionMetadataExtractor
{
    private XPathExpression xFileName;
    

    /**
     * Constructor
     * @throws XPathExpressionException 
     * @throws Exception an exception
     */
    public CollectionMetadataExtractor() throws XPathExpressionException
    {
        XPathFactory xpf = XPathFactory.newInstance();
        xFileName = XPathUtils.compileXPath(xpf, "//*[local-name() = 'File_Area_Inventory']/*[local-name() = 'File']/*[local-name() = 'file_name']");
    }
    

    /**
     * Extract collection inventory file names
     * @param doc Parsed collection label (XML DOM)
     * @return a set of files (usually there is only one inventory file)
     * @throws XPathExpressionException 
     * @throws Exception an exception
     */
    public Set<String> extractInventoryFileNames(Document doc) throws XPathExpressionException
    {
        return XPathUtils.getStringSet(doc, xFileName);
    }
}
