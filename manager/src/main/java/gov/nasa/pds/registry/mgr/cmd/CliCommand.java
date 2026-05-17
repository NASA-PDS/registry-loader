package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.mgr.util.HarvestConfigReader;

/**
 * All Registry Manager command-line interface (CLI) commands such as
 * "create-registry", "delete-registry", "load-data", etc.
 * should implement this interface.
 *
 * @author karpenko
 */
public interface CliCommand
{
    Logger LOG = LogManager.getLogger(CliCommand.class);

    /**
     * Run CLI command.
     * @param cmdLine Command line parameters.
     * @throws Exception an exception
     */
    public void run(CommandLine cmdLine) throws Exception;

    /**
     * Returns {registryUrl, authFile} from either -c (harvest config XML) or
     * the deprecated -registry / -auth flags.
     */
    public static String[] getConfigPair(CommandLine cmdLine) throws Exception {
        if (cmdLine.hasOption("c")) {
            return HarvestConfigReader.readRegistryAndAuth(new File(cmdLine.getOptionValue("c")));
        }
        if (cmdLine.hasOption("registry")) {
            LOG.warn("-registry is deprecated; use -c <harvest-config.xml> instead");
            if (cmdLine.hasOption("auth")) {
                LOG.warn("-auth is deprecated; use -c <harvest-config.xml> instead");
            }
            return new String[]{cmdLine.getOptionValue("registry"), cmdLine.getOptionValue("auth")};
        }
        throw new IllegalArgumentException("Must provide -c <harvest-config.xml> on the command line. See usage for more details.");
    }

    /** Returns the registry connection URL from -c or the deprecated -registry flag. */
    public static String getRegistryUrl(CommandLine cmdLine) throws Exception {
        return getConfigPair(cmdLine)[0];
    }

    /** Returns the auth file path from -c or the deprecated -auth flag. */
    public static String getAuthFile(CommandLine cmdLine) throws Exception {
        return getConfigPair(cmdLine)[1];
    }

    /** @deprecated Use getRegistryUrl(cmdLine) instead. */
    @Deprecated(since = "1.3.0")
    public static String getUsersRegistry(CommandLine cmdLine) {
        try {
            return getConfigPair(cmdLine)[0];
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }
}
