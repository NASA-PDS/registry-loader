package suite;

import org.apache.commons.exec.CommandLine;
import gov.nasa.pds.harvest.HarvestMain;
import mock.OpensearchSupportedFunctionality;

public class Harvest extends ArtificialComposite implements CliAware, OpensearchSupportedFunctionality {
  private String args = null;
  @Override
  public void run() {
    String[] args = {};
    if (this.args != null && !this.args.isBlank()) {
      args = CommandLine.parse(this.args).getArguments();
    }
    HarvestMain.main(args);
  }
  @Override
  public void arguments(String args) {
    this.args = args;
  }
}
