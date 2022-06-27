package gov.nasa.pds.registry.mgr.dao;

public class IndexSettings
{
    public int shards;
    public int replicas;
    
    public IndexSettings()
    {
        shards = -1;
        replicas = -1;
    }
}
