Feature: 1.4.x
  Scenario Outline: NASA-PDS/registry-loader#<issueNumber>-<subtest>
    Given registry-loader issue <issueNumber>, test <subtest>, and opensearch mocks <mocks>
    When test suite <suite> is executed with CLI arguments <cliargline>
    Then compare to the expected outcome <expectation>.
    @1.4.x
    Examples:
      | issueNumber | subtest |       mocks          |      suite      | cliargline |  expectation  |
      |    139      |    0    | "mocks.osf.Standard" | "suites.Sanity" |     ""     | "expect.Sane" |

| 139 | 0 | "mocks.osf.Standard,mocks.osf.JUnitish" | "suites.Sanity" |  ""  | "expect.Sane" |
