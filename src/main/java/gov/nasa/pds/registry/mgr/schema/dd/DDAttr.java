package gov.nasa.pds.registry.mgr.schema.dd;

/**
 * Data dictionary attribute (attribute) 
 * @author karpenko
 */
public class DDAttr
{
    public String id;
    // <namespace>.<name>
    public String nsName;
    
    public DDAttr(String id)
    {
        this.id = id;
    }
}
