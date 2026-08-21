package suite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import mock.OpensearchSupportedFunctionality;
import mock.osf.JUnitish;

public final class Sanity extends ArtificialComposite {
  private final List<OpensearchSupportedFunctionality> knownMocks = new ArrayList<OpensearchSupportedFunctionality>();

  @Override
  public void mocks(List<OpensearchSupportedFunctionality> mocks) {
    super.mocks(mocks);
    this.knownMocks.addAll(mocks);
  }

  @Override
  public void run() {
    System.out.println("running sanity");
    System.out.println("mocks: " + this.knownMocks);
    for (OpensearchSupportedFunctionality osf : this.knownMocks) {
      if (osf instanceof JUnitish) {
        System.out.println("running junitish");
        ((JUnitish) osf).runTests(this);
      }
    }
  }

  @Test
  public void testAuthorize() {
    HashMap<String,String> headers = new HashMap<String,String>();
    Context ctx = new Context("test", "body", headers, null, null);
    headers.put("user-agent", "opensearch-java/3.2.0 (Java/21.0.11)");
    headers.put("accept", "application/json; charset=UTF-8");
    headers.put("authorization", "Basic fakekey");
    headers.put("content-type", "application/json; charset=UTF-8");
      assert this.authorize(ctx).statusCode() == 200 : "authorization was not requested";
  }
  
  @Test
  public void testMappingSettings() {
    Context ctx = new Context("test", "body", null, null, null);
    assert this.putMappingsSettings(ctx).statusCode() == 200 : "did not return a success status code";
  }
}
