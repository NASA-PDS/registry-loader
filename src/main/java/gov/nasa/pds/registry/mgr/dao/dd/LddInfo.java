package gov.nasa.pds.registry.mgr.dao.dd;

import java.time.Instant;

/**
 * LDD information
 * @author karpenko
 */
public class LddInfo implements Comparable<LddInfo>
{
    public String namespace;
    public String file;
    public Instant date;
    
    
    @Override
    public int compareTo(LddInfo o)
    {
        if(namespace.equals(o.namespace))
        {
            if(date == null) return -1;
            if(o.date == null) return 1;
            return date.compareTo(o.date);
        }
        else
        {
            return namespace.compareTo(o.namespace);
        }
    }
    
    
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof LddInfo)
        {
            return file.equals(((LddInfo) o).file);
        }
        else 
        {
            return false;
        }
    }
    
    
    @Override
    public int hashCode()
    {
        return file.hashCode();
    }
}
