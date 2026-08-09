package cucumber;

import java.util.ArrayList;
import io.cucumber.java.After;
import io.cucumber.java.AfterAll;
import io.cucumber.java.BeforeAll;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import mock.MockAware;
import mock.OpensearchEngine;
import mock.OpensearchSupportedFunctionality;
import suite.CliAware;

public class StepDefs {
  private final ArrayList<OpensearchSupportedFunctionality> mocks = new ArrayList<OpensearchSupportedFunctionality>();
  private final OpensearchEngine motor = new OpensearchEngine();

  @After
  public void reset() {
    this.mocks.clear();
    this.motor.clear();
  }

  @AfterAll
  public void stop() {
    motor.stop();
  }

  @BeforeAll
  public void start() {
    motor.start(19022);
  }

  @Given("registry-loader issue {int}, test {int}, and opensearch mocks {string}")
  public void construct(Integer issueNumber, Integer count, String mocks) {
    for (String mock : mocks.split(",")) {
      this.mocks.add((OpensearchSupportedFunctionality)classForName(mock));
    }
  }

  @When("test suite {string} is executed with CLI arguments {string}")
  public void execute(String suite, String cliargline) {
    Object obj = classForName(suite);
    if (obj instanceof CliAware) {
      ((CliAware)obj).arguments(cliargline);
    } else if (cliargline != null && !cliargline.isBlank()) {
      throw new IllegalStateException("An argument line was given to the suite " + suite + " that is not suite.CliAware");
    }
    if (obj instanceof Runnable) {
      ((Runnable)obj).run();
    } else {
      throw new IllegalStateException("The suite " + suite + " is not Runnable.");
    }
  }

  @Then("compare to the expected outcome {string}.")
  public void compare(String expectation) {
    Object obj = this.classForName(expectation);
    if (obj instanceof Runnable) {
      ((Runnable)obj).run();
    }
  }
  
  private Object classForName(String className) {
    try {
      Class<?> clazz = Class.forName(className.trim());
      Object obj = clazz.getDeclaredConstructor().newInstance();
      if (obj instanceof MockAware) {
        ((MockAware)obj).mocks(this.mocks);
      }
      return obj;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Failed to instantiate mock class '" + className + "' — check spelling/package in the feature file's mocks column.", e
          );
    }
  }
}
