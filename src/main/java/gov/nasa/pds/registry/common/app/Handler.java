package gov.nasa.pds.registry.common.app;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {

  @Override
  protected URLConnection openConnection(URL u) throws IOException {
    System.out.println("here");
    return null;
  }

}
