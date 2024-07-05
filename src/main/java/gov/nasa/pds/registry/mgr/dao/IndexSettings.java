package gov.nasa.pds.registry.mgr.dao;

import gov.nasa.pds.registry.common.Response;

public class IndexSettings
{
    public int shards;
    public int replicas;
    public IndexSettings() {
        shards = -1;
        replicas = -1;
    }
    public IndexSettings(Response.Settings resp) {
      this.replicas = resp.replicas();
      this.shards = resp.shards();
    }
}
