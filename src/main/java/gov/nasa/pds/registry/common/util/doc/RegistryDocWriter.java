package gov.nasa.pds.registry.common.util.doc;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.FieldMap;
import gov.nasa.pds.registry.common.util.json.NJsonDocUtils;


/**
 * A class to write metadata extracted from PDS4 label.
 *  
 * @author karpenko
 */
public class RegistryDocWriter implements Closeable
{
    private List<String> jsonData;

    /**
     * Constructor
     */
    public RegistryDocWriter()
    {
        jsonData = new ArrayList<>();
    }

    
    /**
     * Get NJSON data to be loaded into Elasticsearch
     * @return NJSON data (Two JSON entries per Elasticsearch document - (1) id, (2) data.
     */
    public List<String> getData()
    {
        return jsonData;
    }
    

    public void clearData()
    {
        jsonData.clear();
    }
    
    
    /**
     * Write metadata extracted from PDS4 labels.
     * @param meta metadata extracted from PDS4 label.
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void write(Metadata meta, String jobId) throws Exception
    {
        // First line: primary key 
        String lidvid = meta.lid + "::" + meta.vid;
        String pkJson = NJsonDocUtils.createPKJson(lidvid);
        jsonData.add(pkJson);
        
        // Second line: main document

        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);
        
        jw.beginObject();

        // Basic info
        NJsonDocUtils.writeField(jw, "lid", meta.lid);
        NJsonDocUtils.writeField(jw, "vid", meta.strVid);
        NJsonDocUtils.writeField(jw, "lidvid", lidvid);
        NJsonDocUtils.writeField(jw, "title", meta.title);
        NJsonDocUtils.writeField(jw, "product_class", meta.prodClass);

        // Transaction ID
        NJsonDocUtils.writeField(jw, "_package_id", jobId);
        
        // References
        write(jw, meta.intRefs);
        
        // Other Fields
        write(jw, meta.fields);
        
        jw.endObject();
        
        jw.close();

        String dataJson = sw.getBuffer().toString();
        jsonData.add(dataJson);
    }


    private void write(JsonWriter jw, FieldMap fmap) throws Exception
    {
        if(fmap == null || fmap.isEmpty()) return;
        
        for(String key: fmap.getNames())
        {
            Collection<String> values = fmap.getValues(key);
            
            // Skip empty single value fields
            if(values.size() == 1 && values.iterator().next().isEmpty())
            {
                continue;
            }

            NJsonDocUtils.writeField(jw, key, values);
        }
    }


    @Override
    public void close() throws IOException
    {
    }

    
}
