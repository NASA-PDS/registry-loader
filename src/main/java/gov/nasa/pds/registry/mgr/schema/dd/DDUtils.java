package gov.nasa.pds.registry.mgr.schema.dd;

public class DDUtils
{
    public static String stripAuthorityId(String str)
    {
        if(str == null) return null;
        
        int idx = str.indexOf('.');
        return (idx > 0) ? str.substring(idx + 1) : str;
    }

}
