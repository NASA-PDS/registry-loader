package gov.nasa.pds.registry.common.meta;

import java.util.HashSet;
import java.util.Set;

/**
 * A cache of field names in Elasticsearch schema for the registry index.
 * Implemented as a singleton.
 * @author karpenko
 */
public class FieldNameCache
{
    private static FieldNameCache singleton = new FieldNameCache();
    
    private Set<String> schemaFieldNames;
    private Set<String> boolFieldNames;
    private Set<String> dateFieldNames;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     */
    private FieldNameCache()
    {
        schemaFieldNames = new HashSet<>();
        boolFieldNames = new HashSet<>();
        dateFieldNames = new HashSet<>();
    }

    
    /**
     * Get the singleton instance 
     * @return field cache instance
     */
    public static FieldNameCache getInstance()
    {
        return singleton;
    }
    
    
    /**
     * Set field names present in "registry" Elasticsearch schema
     * @param fieldNames collection of field names
     */
    public void setSchemaFieldNames(Set<String> fieldNames)
    {
        this.schemaFieldNames = fieldNames;
    }

    
    /**
     * Set boolean field names present in LDDs
     * @param fieldNames collection of field names
     */
    public void setBooleanFieldNames(Set<String> fieldNames)
    {
        this.boolFieldNames = fieldNames;
    }

    
    /**
     * Set date field names present in LDDs
     * @param fieldNames collection of field names
     */
    public void setDateFieldNames(Set<String> fieldNames)
    {
        this.boolFieldNames = fieldNames;
    }

    
    /**
     * Check if a field name is in the "registry" Elasticsearch schema.
     * @param name field name
     * @return true if field name is in "registry" schema.
     */
    public boolean schemaContainsField(String name)
    {
        return schemaFieldNames.contains(name);
    }

    
    /**
     * Check if a field is a boolean field.
     * @param name field name
     * @return true if this is a boolean field
     */
    public boolean isBooleanField(String name)
    {
        return boolFieldNames.contains(name);
    }


    /**
     * Check if a field is a date field.
     * @param name field name
     * @return true if this is a date field
     */
    public boolean isDateField(String name)
    {
        return dateFieldNames.contains(name);
    }

}
