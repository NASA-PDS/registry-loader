package gov.nasa.pds.registry.common.app;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {
  @Override
  protected void parseURL(URL u, String spec, int start, int limit) {
    /*
     * Ugly backwards compatibility. Flip any file separator
     * characters to be forward slashes. This is a nop on Unix
     * and "fixes" win32 file paths. According to RFC 2396,
     * only forward slashes may be used to represent hierarchy
     * separation in a URL but previous releases unfortunately
     * performed this "fixup" behavior in the file URL parsing code
     * rather than forcing this to be fixed in the caller of the URL
     * class where it belongs. Since backslash is an "unwise"
     * character that would normally be encoded if literally intended
     * as a non-seperator character the damage of veering away from the
     * specification is presumably limited.
     */
    super.parseURL(u, spec.replace(File.separatorChar, '/'), start, limit);
  }
  @Override
  protected URLConnection openConnection(URL u) throws IOException {
    System.out.println("here");
    return null;
  }
}
