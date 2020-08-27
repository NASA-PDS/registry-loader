package gov.nasa.pds.registry.mgr;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RegistryManagerMain
{
    public static void main(String[] args)
    {
        Logger log = Logger.getLogger("");
        log.setLevel(Level.OFF);
        
        RegistryManagerCli cli = new RegistryManagerCli();
        cli.run(args);
    }

}
