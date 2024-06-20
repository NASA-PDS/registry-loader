package gov.nasa.pds.registry.common.connection;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public final class KnownRegistryConnections {
  static { initialzeAppHandler(); }
  static public void initialzeAppHandler() {
    String handlers = System.getProperty("java.protocol.handler.pkgs");
    if (handlers == null) {
      handlers = "gov.nasa.pds.registry.common";
    } else if (!handlers.contains("gov.nasa.pds.registry.common")) {
      handlers += "|gov.nasa.pds.registry.common";
    }
    System.setProperty("java.protocol.handler.pkgs", handlers);
  }
  static public List<URL> list() throws URISyntaxException, IOException {
    return KnownRegistryConnections.list(KnownRegistryConnections.class);
  }
  static public List<URL> list(@SuppressWarnings("rawtypes") Class cls) throws URISyntaxException, IOException {
    ArrayList<URL> result = new ArrayList<URL>();
    Path root;
    URL resource = cls.getResource("/connections");
    if (resource == null) {
      throw new RuntimeException("Resource files are not packaged in the jar file containing class " + cls.getName());
    }
    if (resource.getProtocol().equalsIgnoreCase("jar")) {
      FileSystem jarfiles = FileSystems.newFileSystem(resource.toURI(), Collections.<String,Object>emptyMap());
      root = jarfiles.getPath("/connections");
    } else {
      root = Paths.get(resource.toURI());
    }
    try (Stream<Path> walk = Files.walk(root, Integer.MAX_VALUE)) {
      for (Iterator<Path> i = walk.iterator() ; i.hasNext();) {
        String candidate = i.next().toString();
        candidate = candidate.substring(candidate.indexOf("/connections"));
        if (candidate.endsWith(".xml")) {
          result.add(new URL("app:/" + candidate));
        }
      }
    }
    return result;
  }
}
