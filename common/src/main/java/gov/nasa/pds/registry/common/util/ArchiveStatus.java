package gov.nasa.pds.registry.common.util;

import java.util.Set;
import java.util.TreeSet;

public class ArchiveStatus {
  final public Set<String> statusNames;
  public ArchiveStatus() {
    statusNames = new TreeSet<>();
    statusNames.add("staged");
    statusNames.add("archived");
    statusNames.add("certified");
    statusNames.add("restricted");
  }
  public void validateStatusName (String status) throws Exception {
    if (!statusNames.contains(status)) {
      String authorized_status = String.join(", ", this.statusNames);
      throw new Exception("Invalid parameter value: '" + status + "'. Authorized values are "
          + authorized_status + ".");
    }   
  }
}
