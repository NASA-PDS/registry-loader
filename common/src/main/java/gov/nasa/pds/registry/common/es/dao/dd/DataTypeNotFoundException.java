package gov.nasa.pds.registry.common.es.dao.dd;

/**
 *  Throw this exception when the data type for a new registry field 
 *  is not found in the data dictionary and Elasticsearch schema 
 *  could not be updated.
 *  
 * @author karpenko
 */
@SuppressWarnings("serial")
public class DataTypeNotFoundException extends Exception
{
    private static final String LINK 
        = "See 'https://nasa-pds.github.io/pds-registry-app/operate/common-ops.html#Load' for more information.";
    
    /**
     * Constructor
     */
    public DataTypeNotFoundException()
    {
        super("Could not find datatype(s). " + LINK);
    }
    
    /**
     * Constructor
     * @param fieldName Elasticsearch field name
     */
    public DataTypeNotFoundException(String fieldName)
    {
        super("Could not find datatype for field '" + fieldName + "'. " + LINK);
    }
}
