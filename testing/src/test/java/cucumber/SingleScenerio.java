package cucumber;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SingleScenerio {
  public static final String TEST_DATA_DIR =
      System.getProperty("user.dir") + File.separator + "src/test/resources";

  private static String clean(String arg) {
    arg = arg.strip();
    return arg.substring(1,arg.length()-1);
  }
  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length > 2 || args.length == 0) {
      System.out.println ("usage: <issue number> <subtest>");
      System.out.println ("  <issue number> is the first column in the feature file like 1066");
      System.out.println ("  <subtest> is the second column and should be omitted if there are no subtests for the <issue number>");
      return;
    }

    for (File file : Paths.get(TEST_DATA_DIR, "features").toFile().listFiles((dir, name) -> name.endsWith(".feature"))) {
      for (String line : Files.readAllLines(file.toPath())) {
        line = line.strip();
        if (line.startsWith("|")) {
          String[] scenerio = line.split("\\|");
          if (scenerio[1].strip().equals(args[0])) {
            if (args.length == 1 && !scenerio[2].strip().isBlank()) {
              System.out.println ("Scenerio " + args[0] + " requires a subtest value too.");
              return;
            }
            if (args.length == 2 && scenerio[2].strip().isBlank()) {
              System.out.println ("Scenerio " + args[0] + " does not require a subtest value.");
              return;
            }
            if (args.length == 2 && !args[1].equals(scenerio[2].strip())) continue;
            StepDefs engine = new StepDefs();
            System.out.println("construct");
            engine.construct(
                Integer.valueOf(scenerio[1].strip()),
                args.length == 1 ? null : Integer.valueOf(scenerio[2].strip()),
                clean(scenerio[3]));
            System.out.println("");
            engine.execute (clean(scenerio[4]), clean(scenerio[5]));
            System.out.println ("compare");
            engine.compare (clean(scenerio[6]));
            System.out.println ("success");
            return;
          }
        }
      }
    }
    System.out.print ("Could not find issue number " + args[0]);
    if (args.length == 2) System.out.print (" and subtest " + args[1]);
    System.out.println();
    return;
  }
}
