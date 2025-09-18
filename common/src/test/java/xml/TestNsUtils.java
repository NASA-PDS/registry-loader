package xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

import gov.nasa.pds.registry.common.util.xml.NsUtils;
import gov.nasa.pds.registry.common.util.xml.XmlDomUtils;
import gov.nasa.pds.registry.common.util.xml.XmlNamespaces;


public class TestNsUtils
{

    public static void main(String[] args) throws Exception
    {
        File file = new File("/tmp/d1/1294638283.xml");

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        Document doc = XmlDomUtils.readXml(dbf, file);

        XmlNamespaces xmlns = NsUtils.getNamespaces(doc);
        xmlns.debug();
    }

}
