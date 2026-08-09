Feature: 1.4.x
  Scenario Outline: NASA-PDS/registry-loader#<issueNumber>-<subtest>
    Given registry-loader issue <issueNumber>, test <subtest>, opensearch mocks <mocks>, test suite <suite>, CLI arguments <args>, expectation <expectation>
    When test suite <suite> executed
    Then compared to the expected outcome <expectation>.
    @1.4.x
    Examples:
      | issueNumber | subtest |       mocks          |      suite      | args |  expectation  |
      |    139      |    0    | "mocks.osf.Standard" | "suites.Sanity" |  ""  | "expect.Sane" |

| 139 | 0 | "mocks.osf.Standard" | "suites.Sanity" |  ""  | "expect.Sane" |
