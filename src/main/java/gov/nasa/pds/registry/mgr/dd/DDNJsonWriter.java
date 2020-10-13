package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import com.google.gson.stream.JsonWriter;
import gov.nasa.pds.registry.mgr.util.json.BaseNJsonWriter;


public class DDNJsonWriter extends BaseNJsonWriter<DDRecord>
{
    public DDNJsonWriter(File file) throws Exception
    {
        super(file);
    }

    
    @Override
    public void writeRecord(JsonWriter jw, DDRecord data) throws Exception
    {
        String fieldName = (data.esFieldName != null) ? data.esFieldName : data.esFieldNameFromComponents();
        if(fieldName == null) throw new Exception("ES field name is null");
        writeField(jw, "es_field_name", fieldName);
        
        if(data.esDataType == null) throw new Exception("ES data type is null");
        writeField(jw, "es_data_type", data.esDataType);

        writeField(jw, "class_ns", data.classNs);
        writeField(jw, "class_name", data.className);
        writeField(jw, "attr_ns", data.attrNs);
        writeField(jw, "attr_name", data.attrName);
        writeField(jw, "data_type", data.dataType);
        writeField(jw, "description", data.description);
    }


    private void writeField(JsonWriter jw, String name, String value) throws Exception
    {
        if(value == null) return;
        jw.name(name).value(value);
    }
}
