package gov.nasa.pds.registry.mgr.util;

/**
 * Common methods to parse command line parameters.
 * @author karpenko
 */
public class ParamParserUtils
{
    /**
     * Parse "yes/no" command line parameter.
     * @param paramName Parameter name used to generate exception message.
     * @param val String value to parse. Can be any of "y", "yes", "n", "no", upper or lower case.
     * @return true for "yes", false for "no".
     * @throws Exception Throw exception if invalid value is passed.
     */
    public static boolean parseYesNo(String paramName, String val) throws Exception
    {
        val = val.toLowerCase();
        
        if(val.equals("y") || val.equals("yes"))
        {
            return true;
        }
        
        if(val.equals("n") || val.equals("no"))
        {
            return false;
        }
        
        throw new Exception("Parameter '" + paramName + "' has invalid value '" + val + "'");
    }

}
