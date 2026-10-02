package expect;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckHarvestLog implements LogAware,Runnable {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private final Map<String, Long> expected = Map.of(
      "Label Failure", 0L,
      "Label Ignored", 0L,
      "Label Matched", 0L,
      "Label Skipped", 0L,
      "Label Success", 21L
  );
  private final Pattern summary = Pattern.compile("SUMMARY (.+?): (-?\\d+)");

  private Path logpath = null;

  @Override
  public void run() {
    assert logpath != null : "Cannot check log if it does not exist.";
    Map<String, Long> found = new HashMap<>();
    try (Stream<String> lines = Files.lines(this.logpath)) {
        lines.map(summary::matcher)
             .filter(Matcher::find)
             .forEach(m -> found.put(m.group(1), Long.parseLong(m.group(2)))); // last one wins
    } catch (IOException e) {
      log.error("Error reading log file {}", this.logpath, e);
      assert false : "Cannot read the harvest log file";
    }
    List<String> errors = new ArrayList<>();
    expected.forEach((label, expected) -> {
        Long actual = found.get(label);
        if (actual == null) {
            errors.add(label + ": expected " + expected + " but entry not found");
        } else if (!actual.equals(expected)) {
            errors.add(label + ": expected " + expected + " does not equal found " + actual);
        }
    });
    if (!errors.isEmpty()) {
        throw new AssertionError(String.join(System.lineSeparator(), errors));
    }
  }
  @Override
  public void logpath(Path path) {
    this.logpath = path.resolve("harvest.log");
  }
}
