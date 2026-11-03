Feature: 1.4.x
  Scenario Outline: NASA-PDS/registry-loader#<issueNumber>-<subtest>
    Given registry-loader issue <issueNumber>, test <subtest>, and opensearch mocks <mocks>
    When test suite <suite> is executed with CLI arguments <cliargline>
    Then compare to the expected outcome <expectation>.
    @1.4.x
    Examples:
      | issueNumber | subtest |       mocks          |      suite      | cliargline |  expectation  |
#     |    139      |    0    | "mock.osf.Standard" | "suite.Sanity" |     ""     | "expect.Sane" |

| 139 | 0 | "mock.osf.Standard,mock.osf.JUnitish" | "suite.Sanity" |  ""  | "expect.Sane" |
| 140 | 1 | "mock.osf.Standard"  | "suite.Harvest" | "--overwrite -c ${REGISTRY_REF_DATA}/conf/naif3.bundles.xml" | "expect.CheckHarvestLog" |
| 140 | 2 | "mock.osf.Standard"  | "suite.Harvest" | "--overwrite -c ${REGISTRY_REF_DATA}/conf/naif3.directories.xml" | "expect.CheckHarvestLog" |
