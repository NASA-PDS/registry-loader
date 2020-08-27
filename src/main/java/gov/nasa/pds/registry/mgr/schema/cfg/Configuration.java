package gov.nasa.pds.registry.mgr.schema.cfg;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Configuration
{
    // List of JSON data dictionary files
    public List<File> dataDicFiles;
    
    public Set<String> includeClasses;
    public Set<String> excludeClasses;
    
    // Key = class name, Value = Custom generator
    public Map<String, File> customClassGens;
    
    public List<File> dataTypeFiles;
}
