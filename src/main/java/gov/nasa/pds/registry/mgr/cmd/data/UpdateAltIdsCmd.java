package gov.nasa.pds.registry.mgr.cmd.data;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.ResponseException;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.cmd.CliCommand;
import gov.nasa.pds.registry.mgr.dao.RegistryDao;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;


/**
 * A CLI command to update product's alternate IDs in Elasticsearch.
 * 
 * @author karpenko
 */
public class UpdateAltIdsCmd implements CliCommand
{
    private Logger log;
    
    /**
     * Constructor
     */
    public UpdateAltIdsCmd()
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }
        String url = cmdLine.getOptionValue("es", "app:/connections/direct/localhost.xml");
        String authFile = cmdLine.getOptionValue("auth");
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) throw new Exception("Missing required parameter '-file'");
        File file = new File(filePath);
        if(!file.exists()) throw new Exception("Input file doesn't exist: " + file.getAbsolutePath());
        
        try
        {
            RegistryManager.init(url, authFile);
            updateIds(file);
        }
        catch(ResponseException ex)
        {
            throw new Exception(ex.extractErrorMessage());
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private void updateIds(File file) throws Exception
    {
        BufferedReader rd = null;

        try
        {
            rd = new BufferedReader(new FileReader(file));
            
            String line;
            int lineNum = 0;
            
            while((line = rd.readLine()) != null)
            {
                lineNum++;
                line = line.trim();
                
                String[] ids = StringUtils.split(line, ",;\t ");
                if(ids == null || ids.length != 2)
                {
                    log.warn("Line " + lineNum + " has invalid value: [" + line + "]");
                    continue;
                }
                
                List<String> newIds = new ArrayList<String>(4);
                
                // First LIDVID value (old)
                int idx = ids[0].indexOf("::");
                if(idx < 1)
                {
                    log.warn("Line " + lineNum + " has invalid LIDVID: [" + ids[0] + "]");
                    continue;
                }
                
                // Add LIDVID
                newIds.add(ids[0]);
                // Add LID
                newIds.add(ids[0].substring(0, idx));
                
                // Second LIDVID value (new)
                idx = ids[1].indexOf("::");
                if(idx < 1)
                {
                    log.warn("Line " + lineNum + " has invalid LIDVID: [" + ids[1] + "]");
                    continue;
                }

                // Add LIDVID
                newIds.add(ids[1]);
                // Add LID
                newIds.add(ids[1].substring(0, idx));

                Map<String, List<String>> idMap = new TreeMap<>();
                idMap.put(ids[0], newIds);
                idMap.put(ids[1], newIds);
                
                log.info("Updating " + ids[0] + " -> " + ids[1]);
                updateIds(idMap);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void updateIds(Map<String, List<String>> newIds) throws Exception
    {
        RegistryDao dao = RegistryManager.getInstance().getRegistryDao();
        
        Map<String, Set<String>> existingIds = dao.getAlternateIds(newIds.keySet());
        if(existingIds == null || existingIds.isEmpty()) return;
        
        for(Map.Entry<String, Set<String>> entry: existingIds.entrySet())            
        {
            List<String> additionalIds = newIds.get(entry.getKey());
            if(additionalIds != null)
            {
                entry.getValue().addAll(additionalIds);
            }
        }
        
        dao.updateAlternateIds(existingIds);
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-alt-ids <options>");

        System.out.println();
        System.out.println("Update product's alternate IDs");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>      CSV file with the list of IDs");
        System.out.println("Optional parameters:");
        System.out.println("  -auth <file>      Authentication config file");
        System.out.println("  -es <url>         Elasticsearch URL. Default is app:/connections/direct/localhost.xml");
        System.out.println();
    }

}
