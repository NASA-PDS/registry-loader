package gov.nasa.pds.registry.mgr.schema.dd;

import java.util.ArrayList;
import java.util.List;

/**
 * Data dictionary class 
 * @author karpenko
 */
public class DDClass
{
    // <namespace>.<name>
    public String nsName;
    public List<DDAttr> attributes;
    
    public DDClass(String nsName)
    {
        this.nsName = nsName;
        attributes = new ArrayList<>();
    }
}
