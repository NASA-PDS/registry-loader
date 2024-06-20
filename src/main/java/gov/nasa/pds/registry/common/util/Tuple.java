package gov.nasa.pds.registry.common.util;

/**
 * This class is used to store key, value pairs.
 *  
 * @author karpenko
 */
public class Tuple
{
    public String item1;
    public String item2;
    public Tuple() {
    }
    /**
     * Constructor
     * @param item1 value 1
     * @param item2 value 2
     */
    public Tuple(String item1, String item2)
    {
        this.item1 = item1;
        this.item2 = item2;
    }
}
