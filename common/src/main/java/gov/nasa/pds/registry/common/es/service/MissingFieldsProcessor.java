package gov.nasa.pds.registry.common.es.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import gov.nasa.pds.registry.common.meta.FieldNameCache;
import gov.nasa.pds.registry.common.meta.MetaConstants;
import gov.nasa.pds.registry.common.util.FieldMap;
import gov.nasa.pds.registry.common.util.xml.XmlNamespaces;


/**
 * Process fields present in PDS4 label, but missing from Elasticsearch "registry" index.
 * @author karpenko
 */
public class MissingFieldsProcessor
{
    private Set<String> missingFields;
    private Map<String, String> missingXsds;

    final private SchemaUpdater schemaUpdater;
    final private FieldNameCache fieldNameCache;
    
    
    /**
     * Constructor.
     * NOTE: Init registry manager before calling this constructor
     * @param su schema updater
     * @param fnc field name cache
     * @throws Exception an exception
     */
    public MissingFieldsProcessor(SchemaUpdater su, FieldNameCache fnc) throws Exception
    {
        missingFields = new HashSet<>();
        missingXsds = new HashMap<>();
        
        this.schemaUpdater = su;
        this.fieldNameCache = fnc;
    }

    
    /**
     * Loop through all fields in a PDS label and if any fields are missing from
     * Elasticsearch "registry" schema, update the schema and data dictionary index.
     * @param fmap fields from PDS4 label
     * @param xmlns namespaces and schema locations from PDS4 label. 
     * @throws Exception an exception
     */
    public void processDoc(FieldMap fmap, XmlNamespaces xmlns) throws Exception
    {
        // Find fields not in Elasticsearch "registry" schema
        for(String key: fmap.getNames())
        {
            // Check if current Elasticsearch schema has this field.
            if(!fieldNameCache.schemaContainsField(key))
            {
                // Update missing fields and XSDs
                missingFields.add(key);
                updateMissingXsds(key, xmlns);
            }
        }
        
        // Update LDDs and schema
        if(!missingFields.isEmpty())
        {
            try
            {
                schemaUpdater.updateSchema(missingFields, missingXsds);
                fieldNameCache.update();
            }
            finally
            {
                missingFields.clear();
                missingXsds.clear();
            }
        }
    }
    
    
    protected void updateMissingXsds(String name, XmlNamespaces xmlns)
    {
        int idx = name.indexOf(MetaConstants.NS_SEPARATOR);
        if(idx <= 0) return;
        
        String prefix = name.substring(0, idx);
        String xsd = xmlns.prefix2location.get(prefix);
 
        if(xsd != null)
        {
            missingXsds.put(xsd, prefix);
        }
    }

        
}
