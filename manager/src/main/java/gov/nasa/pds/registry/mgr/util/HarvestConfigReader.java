package gov.nasa.pds.registry.mgr.util;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Reads the &lt;registry&gt; element from a Harvest configuration XML file,
 * returning the registry connection URL and optional auth file path.
 */
public class HarvestConfigReader {

    /**
     * Parses a Harvest config XML and returns {registryUrl, authFile}.
     * authFile may be null if the auth attribute is absent or empty.
     */
    public static String[] readRegistryAndAuth(File configFile) throws Exception {
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(configFile);
        NodeList nodes = doc.getElementsByTagName("registry");
        if (nodes.getLength() == 0) {
            throw new Exception("No <registry> element found in: " + configFile.getAbsolutePath());
        }
        Element regEl = (Element) nodes.item(0);
        String url = regEl.getTextContent().trim();
        if (url.isEmpty()) {
            throw new Exception("<registry> element has no URL value in: " + configFile.getAbsolutePath());
        }
        String auth = regEl.getAttribute("auth").trim();
        return new String[]{url, auth.isEmpty() ? null : auth};
    }
}
