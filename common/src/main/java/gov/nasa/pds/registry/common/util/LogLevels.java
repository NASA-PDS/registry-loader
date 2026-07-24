package gov.nasa.pds.registry.common.util;

import org.apache.logging.log4j.Level;

public class LogLevels {
  private LogLevels() {}
  public static final Level LABEL_FAILURE = Level.forName("LABEL FAILURE", 121); // batch did not accept lidvid
  public static final Level LABEL_IGNORED = Level.forName("LABEL IGNORED", 122); // file could not be read as a label
  public static final Level LABEL_MATCHED = Level.forName("LABEL MATCHED", 123); // batch rejected as duplicate lidvid
  public static final Level LABEL_SKIPPED = Level.forName("LABEL SKIPPED", 124); // the lidvid was already in database
  public static final Level LABEL_SUCCESS = Level.forName("LABEL SUCCESS", 125); // batch accepted lidvid
}
