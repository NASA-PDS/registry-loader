package gov.nasa.pds.registry.common.connection.aws;

import java.util.HashSet;
import java.util.Set;
import gov.nasa.pds.registry.common.Response.Get.IdSets;

class IdSetsImpl implements IdSets {
  final Set<String> lids = new HashSet<String>();
  final Set<String> lidvids = new HashSet<String>();
  @Override
  public Set<String> lids() {
    return this.lids;
  }
  @Override
  public Set<String> lidvids() {
    return this.lidvids;
  }
}
