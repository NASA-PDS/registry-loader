package suite;

import java.nio.file.Path;
import org.apache.commons.exec.CommandLine;
import gov.nasa.pds.harvest.HarvestMain;
import mock.OpensearchSupportedFunctionality;

public class Harvest extends ArtificialComposite implements CliAware, OpensearchSupportedFunctionality {
  private Path logpath = null;
  private String args = null;
  @Override
  public void run() {
    String[] args = {};
    if (this.args != null && !this.args.isBlank()) {
      args = CommandLine.parse(this.args).getArguments();
    }
    if (this.logpath != null) {
      final String[] fargs = args;
      CliAware.teeOutputForOperation(logpath, ()->HarvestMain.main(fargs));
    } else {
      HarvestMain.main(args);
    }
  }
  @Override
  public void arguments(String args) {
    this.args = args;
  }
  @Override
  public void logpath(Path path) {
    this.logpath = path.resolve("harvest.log");
  }
}
