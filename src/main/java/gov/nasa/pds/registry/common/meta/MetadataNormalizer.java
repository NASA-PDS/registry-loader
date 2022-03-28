package gov.nasa.pds.registry.common.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.pds.registry.common.util.FieldMapList;
import gov.nasa.pds.registry.common.util.date.PdsDateConverter;


/**
 * Normalizes (converts) date and boolean values into formats acceptable 
 * by Elasticsearch (registry). 
 * @author karpenko
 */
public class MetadataNormalizer
{
    private PdsDateConverter dateConverter;
    private FieldNameCache fieldNameCache;
    
    /**
     * Constructor
     */
    public MetadataNormalizer(FieldNameCache cache)
    {
        dateConverter = new PdsDateConverter(true);
        this.fieldNameCache = cache;
    }

    
    /**
     * Normalize field values
     * @param fields Metadata extracted from PDS4 label
     */
    public void normalizeValues(FieldMapList fields) throws Exception
    {
        for(String key: fields.getNames())
        {
            // Convert date fields
            if(fieldNameCache.isDateField(key))
            {
                Collection<String> oldValues = fields.getValues(key);
                if(oldValues == null) continue; 
                
                List<String> newValues = convertDateValues(key, oldValues);
                fields.setValues(key, newValues);
            }
            // Convert boolean fields
            else if(fieldNameCache.isBooleanField(key))
            {
                Collection<String> oldValues = fields.getValues(key);
                if(oldValues == null) continue; 
                
                List<String> newValues = convertBooleanValues(key, oldValues);
                fields.setValues(key, newValues);
            }
        }
    }

    
    private List<String> convertDateValues(String key, Collection<String> oldValues) throws Exception
    {
        List<String> newValues = new ArrayList<>();
        
        for(String oldValue: oldValues)
        {
            try
            {
                String newValue = dateConverter.toIsoInstantString(key, oldValue);
                newValues.add(newValue);
            }
            catch(Exception ex)
            {
                throw new Exception("Could not convert date value. Field = " + key + ", value = " + oldValue); 
            }
        }
        
        return newValues;
    }


    private List<String> convertBooleanValues(String key, Collection<String> oldValues) throws Exception
    {
        List<String> newValues = new ArrayList<>();
        
        for(String oldValue: oldValues)
        {
            String tmp = oldValue.toLowerCase();
            
            if("true".equals(tmp) || "1".equals(tmp))
            {
                newValues.add("true");
            }
            else if("false".equals(tmp) || "0".equals(tmp))
            {
                newValues.add("false");
            }
            else
            {
                throw new Exception("Could not convert boolean value. Field = " + key + ", value = " + oldValue); 
            }
        }
        
        return newValues;
    }

}
