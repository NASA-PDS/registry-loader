package gov.nasa.pds.registry.mgr.schema.cfg;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import gov.nasa.pds.registry.mgr.util.XPathUtils;
import gov.nasa.pds.registry.mgr.util.XmlDomUtils;


public class ConfigReader
{
    public ConfigReader()
    {
    }
    
    
    public Configuration read(File file) throws Exception
    {
        Document doc = XmlDomUtils.readXml(file);
        String rootElement = doc.getDocumentElement().getNodeName();
        if(!"schemaGen".equals(rootElement))
        {
            throw new Exception("Invalid root element '" + rootElement + "'. Expecting 'schemaGen'.");
        }
        
        Configuration cfg = new Configuration();
        parseDataDictionary(doc, cfg);
        parseClassFilters(doc, cfg);
        parseCustomGenerators(doc, cfg);
        parseDataTypes(doc, cfg);
        
        return cfg;
    }


    private void parseDataDictionary(Document doc, Configuration cfg) throws Exception
    {
        XPathUtils xpu = new XPathUtils();
        
        int count = xpu.getNodeCount(doc, "/schemaGen/dataDictionary");
        if(count == 0) throw new Exception("Missing required element '/schemaGen/dataDictionary'.");
        if(count > 1) throw new Exception("Could not have more than one '/schemaGen/dataDictionary' element.");
        
        List<String> files = xpu.getStringList(doc, "/schemaGen/dataDictionary/file");
        if(files == null || files.isEmpty()) 
        {
            throw new Exception("Please provide at least one data dictionary file ('/schemaGen/dataDictionary/file')."); 
        }

        Node rootNode = xpu.getFirstNode(doc, "/schemaGen/dataDictionary");
        String baseDirStr = XmlDomUtils.getAttribute(rootNode, "baseDir");
        
        File baseDir = (baseDirStr != null && !baseDirStr.isEmpty()) ? new File(baseDirStr) : null;
        
        List<File> ddFiles = new ArrayList<>();
        for(String str: files)
        {
            File file = (baseDir == null) ? new File(str) : new File(baseDir, str);
            ddFiles.add(file);
        }
        
        cfg.dataDicFiles = ddFiles;
    }
    
    
    private void parseClassFilters(Document doc, Configuration cfg) throws Exception
    {
        XPathUtils xpu = new XPathUtils();
        
        cfg.includeClasses = xpu.getStringSet(doc, "/schemaGen/classFilters/include");
        cfg.excludeClasses = xpu.getStringSet(doc, "/schemaGen/classFilters/exclude");
        
        if(cfg.includeClasses != null && cfg.includeClasses.size() > 0 
                && cfg.excludeClasses != null && cfg.excludeClasses.size() > 0)
        {
            throw new Exception("<classFilters> could not have both <include> and <exclude> at the same time.");
        }
    }
    

    private void parseCustomGenerators(Document doc, Configuration cfg) throws Exception
    {
        XPathUtils xpu = new XPathUtils();
        
        int count = xpu.getNodeCount(doc, "/schemaGen/customGenerators");
        if(count > 1) throw new Exception("Could not have more than one '/schemaGen/customGenerators' element.");
        if(count != 1) return;

        NodeList nodes = xpu.getNodeList(doc, "/schemaGen/customGenerators/class");
        if(nodes == null || nodes.getLength() == 0) return;
        
        Node rootNode = xpu.getFirstNode(doc, "/schemaGen/customGenerators");
        String baseDirStr = XmlDomUtils.getAttribute(rootNode, "baseDir");
        File baseDir = (baseDirStr != null && !baseDirStr.isEmpty()) ? new File(baseDirStr) : null;

        Map<String, File> map = new TreeMap<>(); 
        for(int i = 0; i < nodes.getLength(); i++)
        {
            String className = XmlDomUtils.getAttribute(nodes.item(i), "name");
            if(className == null || className.isBlank()) 
            {
                throw new Exception("//customGenerators/class[" +  i + "] missing attribute 'name'.");
            }
            
            String filePath = XmlDomUtils.getAttribute(nodes.item(i), "file");
            if(className == null || className.isBlank()) 
            {
                throw new Exception("//customGenerators/class[" +  i + "] missing attribute 'file'.");
            }
            
            File file = (baseDir == null) ? new File(filePath) : new File(baseDir, filePath);
            map.put(className, file);
        }

        cfg.customClassGens = map;
    }
    
    
    private void parseDataTypes(Document doc, Configuration cfg) throws Exception
    {
        XPathUtils xpu = new XPathUtils();
        
        int count = xpu.getNodeCount(doc, "/schemaGen/dataTypes");
        if(count > 1) throw new Exception("Could not have more than one '/schemaGen/dataTypes' element.");
        if(count != 1) return;
        
        List<String> files = xpu.getStringList(doc, "/schemaGen/dataTypes/file");
        if(files == null || files.isEmpty()) return; 

        Node rootNode = xpu.getFirstNode(doc, "/schemaGen/dataTypes");
        String baseDirStr = XmlDomUtils.getAttribute(rootNode, "baseDir");
        File baseDir = (baseDirStr != null && !baseDirStr.isEmpty()) ? new File(baseDirStr) : null;
        
        List<File> ddFiles = new ArrayList<>();
        for(String str: files)
        {
            File file = (baseDir == null) ? new File(str) : new File(baseDir, str);
            ddFiles.add(file);
        }
        
        cfg.dataTypeFiles = ddFiles;
    }

}
