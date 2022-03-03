package gov.nasa.pds.registry.common.util.json;

import java.io.StringWriter;
import java.util.Collection;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.FieldMap;

/**
 * Utility class to build NJson strings
 * @author karpenko
 */
public class RegistryDocBuilder
{
    /**
     * Create primary key JSON line of the NJSON 2-line record
     * @param meta PDS4 metadata
     * @return JSON string
     * @throws Exception an exception
     */
    public static String createPKJson(Metadata meta) throws Exception
    {
        String lidvid = meta.lid + "::" + meta.vid;
        String pkJson = NJsonDocUtils.createPKJson(lidvid);
        return pkJson;
    }

    /**
     * Create data JSON line of the NJSON 2-line record
     * @param meta PDS4 metadata
     * @param jobId job id
     * @return JSON string
     * @throws Exception an exception
     */
    public static String createDataJson(Metadata meta, String jobId) throws Exception
    {
        String lidvid = meta.lid + "::" + meta.vid;
        
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
        return dataJson;
    }

    
    private static void write(JsonWriter jw, FieldMap fmap) throws Exception
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

}
