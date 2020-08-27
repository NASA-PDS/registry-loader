package gov.nasa.pds.registry.mgr.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class SchemaGenerator
{
    private Configuration cfg;
    private JsonWriter writer;
    
    private Pds2EsDataTypeMap dtMap;
    private Set<String> existingFieldNames;
    

    public SchemaGenerator(Configuration cfg, JsonWriter writer) throws Exception
    {
        if(cfg == null) throw new IllegalArgumentException("Missing configuration parameter.");
        if(writer == null) throw new IllegalArgumentException("Missing writer parameter.");
        
        this.cfg = cfg;
        this.writer = writer;

        // Load PDS to Elasticsearch data type mapping files
        dtMap = loadDataTypeMap();

        existingFieldNames = new HashSet<String>(2000);
    }

    
    private Pds2EsDataTypeMap loadDataTypeMap() throws Exception
    {
        Pds2EsDataTypeMap map = new Pds2EsDataTypeMap();
        if(cfg.dataTypeFiles != null)
        {
            for(File file: cfg.dataTypeFiles)
            {
                map.load(file);
            }
        }
        
        return map;
    }


    public void generateSolrSchema(DataDictionary dd) throws Exception
    {
        Map<String, String> attrId2Type = dd.getAttributeDataTypeMap();
        Set<String> dataTypes = dd.getDataTypes();
        
        for(DDClass ddClass: dd.getClassMap().values())
        {
            // Skip type definitions.
            if(dataTypes.contains(ddClass.nsName)) continue;
            
            // Apply class filters
            if(cfg.includeClasses != null && cfg.includeClasses.size() > 0)
            {
                if(!cfg.includeClasses.contains(ddClass.nsName)) continue;
            }
            if(cfg.excludeClasses != null && cfg.excludeClasses.size() > 0)
            {
                if(cfg.excludeClasses.contains(ddClass.nsName)) continue;
            }

            File customFile = (cfg.customClassGens == null) ? null : cfg.customClassGens.get(ddClass.nsName);
            if(customFile != null)
            {
                addCustomFields(ddClass, customFile);
            }
            else
            {
                addClassAttributes(ddClass, attrId2Type);
            }
        }
    }
    
    
    private void addCustomFields(DDClass ddClass, File file) throws Exception
    {
        System.out.println("Loading custom generator. Class = " + ddClass.nsName + ", file = " + file.getAbsolutePath());
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(file));
        }
        catch(Exception ex)
        {
            throw new Exception("Could not open custom generator for class '" 
                    + ddClass.nsName + "':  " + file.getAbsolutePath());
        }
        
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                // Skip blank lines and comments
                if(line.isEmpty() || line.startsWith("#")) continue;
                
                // Line format <field name> = <data type>
                String tokens[] = line.split("=");
                if(tokens.length != 2)
                {
                    throw new Exception("Invalid entry: " + line);
                }
                
                String fieldName = tokens[0].trim();
                String fieldType = tokens[1].trim();                
                addEsField(fieldName, fieldType);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void addClassAttributes(DDClass ddClass, Map<String, String> attrId2Type) throws Exception
    {
        for(DDAttr attr: ddClass.attributes)
        {
            String pdsDataType = attrId2Type.get(attr.id);
            if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
            
            String fieldName = ddClass.nsName + "." + attr.nsName;
            String esDataType = dtMap.getEsType(pdsDataType);
            addEsField(fieldName, esDataType);
        }
    }

    
    private void addEsField(String name, String type) throws Exception
    {
        name = name.replaceAll("\\.", Constants.REPLACE_DOT_WITH);
        
        if(existingFieldNames.contains(name)) return;        
        existingFieldNames.add(name);
        
        writer.name(name);
        writer.beginObject();
        writer.name("type").value(type);
        writer.endObject();
    }
}
