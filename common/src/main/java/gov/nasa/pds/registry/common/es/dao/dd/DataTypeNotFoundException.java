package gov.nasa.pds.registry.common.es.dao.dd;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import gov.nasa.pds.registry.common.util.Tuple;

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
        = "See 'https://nasa-pds.github.io/registry/user/load1.html' for more information.";

    private final List<String> missingFields;
    private final List<Tuple> foundTypes;

    public DataTypeNotFoundException()
    {
        super("Could not find datatype(s). " + LINK);
        this.missingFields = Collections.emptyList();
        this.foundTypes = Collections.emptyList();
    }

    public DataTypeNotFoundException(Collection<String> missingFields, List<Tuple> foundTypes)
    {
        super("Could not find datatype(s). " + LINK);
        this.missingFields = Collections.unmodifiableList(List.copyOf(missingFields));
        this.foundTypes = foundTypes != null
            ? Collections.unmodifiableList(List.copyOf(foundTypes))
            : Collections.emptyList();
    }

    /**
     * Constructor
     * @param fieldName Elasticsearch field name
     */
    public DataTypeNotFoundException(String fieldName)
    {
        super("Could not find datatype for field '" + fieldName + "'. " + LINK);
        this.missingFields = Collections.singletonList(fieldName);
        this.foundTypes = Collections.emptyList();
    }

    public List<String> getMissingFields()
    {
        return missingFields;
    }

    public List<Tuple> getFoundTypes()
    {
        return foundTypes;
    }
}
