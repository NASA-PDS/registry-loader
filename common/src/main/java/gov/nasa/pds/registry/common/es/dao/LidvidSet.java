package gov.nasa.pds.registry.common.es.dao;

import java.util.Set;
import gov.nasa.pds.registry.common.Response;

public class LidvidSet implements Response.Get.IdSets
{
    public Set<String> lidvids;
    public Set<String> lids;
    public LidvidSet(Set<String> lids, Set<String> lidvids) {
      this.lids = lids;
      this.lidvids = lidvids;
    }
    public LidvidSet(Response.Get.IdSets ids) {
      this.lids = ids.lids();
      this.lidvids = ids.lidvids();
    }
    @Override
    public Set<String> lids() {
      return this.lids;
    }
    @Override
    public Set<String> lidvids() {
      return this.lidvids;
    }
}
