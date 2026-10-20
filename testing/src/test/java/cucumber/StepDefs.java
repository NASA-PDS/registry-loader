package cucumber;

import java.nio.file.Path;
import java.util.ArrayList;
import expect.LogAware;
import io.cucumber.java.AfterAll;
import io.cucumber.java.Before;
import io.cucumber.java.BeforeAll;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import mock.MockAware;
import mock.OpensearchEngine;
import mock.OpensearchSupportedFunctionality;
import suite.CliAware;

public class StepDefs {
  private int count, issueNumber;
  private final ArrayList<OpensearchSupportedFunctionality> mocks = new ArrayList<OpensearchSupportedFunctionality>();
  private static final OpensearchEngine motor = OpensearchEngine.instance();

  @AfterAll
  public static void stop() {
    StepDefs.motor.stop();
  }

  @Before
  public void reset() {
    StepDefs.motor.clear();
  }

  @BeforeAll
  public static void start() {
    StepDefs.motor.start(9200);
  }

  @Given("registry-loader issue {int}, test {int}, and opensearch mocks {string}")
  public void construct(Integer issueNumber, Integer count, String mocks) {
    this.count = count;
    this.issueNumber = issueNumber;
    for (String mock : mocks.split(",")) {
      this.mocks.add(classForName(mock));
    }
  }

  @When("test suite {string} is executed with CLI arguments {string}")
  public void execute(String suite, String cliargline) {
    Runnable task = this.classForName(suite);
    if (task instanceof CliAware) {
      ((CliAware)task).arguments(cliargline);
      ((CliAware)task).logpath(Path.of("target","test", this.issueNumber + "-" + this.count));
    } else if (cliargline != null && !cliargline.isBlank()) {
      throw new IllegalStateException("An argument line was given to the suite " + suite + " that is not suite.CliAware");
    }
    task.run();
  }

  @Then("compare to the expected outcome {string}.")
  public void compare(String expectation) {
    Runnable expert = this.<Runnable>classForName(expectation);
    if (expert instanceof LogAware) {
      ((LogAware)expert).logpath(Path.of("target","test", this.issueNumber + "-" + this.count));
    }
    expert.run();
  }

  @SuppressWarnings("unchecked")
  private <T> T classForName(String className) {
    try {
      Class<?> clazz = Class.forName(className.trim());
      Object obj = clazz.getDeclaredConstructor().newInstance();
      if (obj instanceof MockAware) {
        ((MockAware)obj).mocks(this.mocks);
      }
     return (T)obj;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed to instantiate mock class '" + className + "' — check spelling/package in the feature file's mocks column.", e
          );
    }
  }
}
