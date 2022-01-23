package gov.nasa.pds.registry.common.meta;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import gov.nasa.pds.registry.common.util.FieldMapList;
import gov.nasa.pds.registry.common.util.FieldMapSet;


/**
 * Metadata extracted from PDS label.
 * 
 * @author karpenko
 */
public class Metadata
{
    public static final String FLD_NODE_NAME = "ops:Harvest_Info/ops:node_name";
    public static final String FLD_HARVEST_DATE_TIME = "ops:Harvest_Info/ops:harvest_date_time";

    
    public String lid;
    public String strVid;
    public float vid;
    public String lidvid;

    public String title;
    public String prodClass;
    
    public FieldMapSet intRefs;
    public FieldMapList fields;
    
    public Set<String> dataFiles;


    /**
     * Constructor
     */
    public Metadata()
    {
        intRefs = new FieldMapSet();
        fields = new FieldMapList();
    }
    

    /**
     * Set node name
     * @param name
     */
    public void setNodeName(String name)
    {
        fields.setValue(FLD_NODE_NAME, name);
    }
    
    
    /**
     * Set harvest timestamp
     * @param val
     */
    public void setHarvestTimestamp(Instant val)
    {
        String strVal = DateTimeFormatter.ISO_INSTANT.format(val);
        fields.setValue(FLD_HARVEST_DATE_TIME, strVal);
    }
}
