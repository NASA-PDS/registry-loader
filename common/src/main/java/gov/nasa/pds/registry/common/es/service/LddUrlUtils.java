package gov.nasa.pds.registry.common.es.service;

import java.net.URL;

/**
 * Utility methods for resolving LDD download URLs.
 */
public class LddUrlUtils
{
    static final String PDS_NASA_GOV = "pds.nasa.gov";

    private LddUrlUtils() {}

    /**
     * Build the pds.nasa.gov mirror URL by replacing the host.
     * Returns null if the URL is already from pds.nasa.gov or cannot be parsed.
     */
    public static String toPdsNasaGovUrl(String url)
    {
        try
        {
            URL parsed = new URL(url);
            if(PDS_NASA_GOV.equals(parsed.getHost()))
            {
                return null;
            }
            return "https://" + PDS_NASA_GOV + parsed.getPath();
        }
        catch(Exception e)
        {
            return null;
        }
    }
}
