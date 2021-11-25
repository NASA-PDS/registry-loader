package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;


/**
 * Simple methods to work with PDS LDD JSON files (data dictionary files).
 *  
 * @author karpenko
 */
public class LddUtils
{
    private static final DateFormat LDD_DateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    
    /**
     * Get default PDS to Elasticsearch data type mapping configuration file.
     * @return File pointing to default configuration file.
     * @throws Exception an exception
     */
    public static File getPds2EsDataTypeCfgFile() throws Exception
    {
        String home = System.getenv("REGISTRY_MANAGER_HOME");
        if(home == null) 
        {
            throw new Exception("Could not find default configuration directory. " 
                    + "REGISTRY_MANAGER_HOME environment variable is not set.");
        }

        File file = new File(home, "elastic/data-dic-types.cfg");
        return file;
    }

    
    /**
     * Convert LDD date, e.g., "Wed Dec 23 10:16:28 EST 2020" 
     * to ISO Instant format, e.g., "2020-12-23T15:16:28Z".
     * @param lddDate LDD date from PDS LDD JSON file.
     * @return ISO Instant formatted date
     * @throws Exception an exception
     */
    public static String lddDateToIsoInstantString(String lddDate) throws Exception
    {
        Date dt = LDD_DateFormat.parse(lddDate);
        return DateTimeFormatter.ISO_INSTANT.format(dt.toInstant());
    }

    
    /**
     * Convert LDD date, e.g., "Wed Dec 23 10:16:28 EST 2020" 
     * to ISO Instant format, e.g., "2020-12-23T15:16:28Z".
     * @param lddDate LDD date from PDS LDD JSON file.
     * @return ISO Instant formatted date
     * @throws Exception an exception
     */
    public static Instant lddDateToIsoInstant(String lddDate) throws Exception
    {
        Date dt = LDD_DateFormat.parse(lddDate);
        return dt.toInstant();
    }
    

    /**
     * Parse JSON LDD file and list all namespaces
     * @param lddFile JSON LDD file
     * @return a set of namespaces
     * @throws Exception an exception
     */
    public static Set<String> listLddNamespaces(File lddFile) throws Exception
    {
        Set<String> namespaces = new TreeSet<>();
        ClassAttrAssociationParser.Callback cb = (classNs, className, attrId) -> 
        {
            namespaces.add(classNs);
        };
        
        ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(lddFile, cb); 
        caaParser.parse();

        return namespaces;
    }

    
    /**
     * Extract LDD namespace. If there are more than one namespace, throw an exception.
     * @param lddFile JSON LDD file
     * @return namespace (e.g., 'pds')
     * @throws Exception an exception
     */
    public static String getLddNamespace(File lddFile) throws Exception
    {
        Set<String> namespaces = LddUtils.listLddNamespaces(lddFile);

        if(namespaces.size() == 1)
        {
            return namespaces.iterator().next();
        }
        
        if(namespaces.size() == 0)
        {
            throw new Exception("Data dictionary doesn't have any namespaces.");
        }
        else
        {
            throw new Exception("Data dictionary has multiple namespaces: " + namespaces 
                                    + ". Specify one namespace to use.");
        }
    }

    
    /**
     * Get JSON LDD URL from XSD location 
     * @param uri XSD location URL
     * @return JSON LDD URL
     * @throws Exception an exception
     */
    public static String getJsonLddUrlFromXsd(String uri) throws Exception
    {
        if(uri.endsWith(".xsd"))
        {
            String jsonUrl = uri.substring(0, uri.length()-3) + "JSON";
            return jsonUrl;
        }
        else
        {
            throw new Exception("Invalid schema URI. URI doesn't end with '.xsd': " + uri);
        }
    }

}

