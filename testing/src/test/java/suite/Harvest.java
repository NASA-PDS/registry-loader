package suite;

import java.nio.file.Path;
import org.apache.commons.exec.CommandLine;
import gov.nasa.pds.harvest.HarvestMain;
import gov.nasa.pds.harvest.dao.RegistryDocBatch;
import gov.nasa.pds.harvest.util.out.WriterManager;
import gov.nasa.pds.registry.common.util.file.FileDownloader;
import mock.OpensearchSupportedFunctionality;

public class Harvest extends ArtificialComposite implements CliAware, OpensearchSupportedFunctionality {
  private Path logpath = null;
  private String args = null;
  @Override
  public void run() {
    String[] args = {};
    if (this.args != null && !this.args.isBlank()) {
      args = CommandLine.parse("harvest " + this.args).getArguments();
    }
    try {
      final String[] fargs = args;
      if (this.logpath != null) {
        CliAware.teeOutputForOperation(logpath, ()->HarvestMain.main(fargs));
        System.out.println("    ***********   logpath: " + logpath.toString());
      } else {
        HarvestMain.main(fargs);
      }
    } finally {
      FileDownloader.clearIgnore();
      RegistryDocBatch.clearHistory();
      WriterManager.destroy(true);
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
