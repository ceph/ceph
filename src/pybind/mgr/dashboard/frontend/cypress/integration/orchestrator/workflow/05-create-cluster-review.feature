Feature: Cluster expansion review section

    Go to review section and verify all the tables and fields
    are present in the section. I also verify that hosts are also there
    in the Host Details table.

    Background: Cluster expansion wizard
        Given I am logged in
        And I am on the "welcome" page
        And I click on "Expand Cluster" button

    Scenario: Verify Cluster Resources table and its fields exists
        Given I am on the "Review" section
        Then I should see "Cluster Resources" heading
        And I should see the following entries in the table
            | fields |
            | Hosts |
            | Storage Capacity |
            | CPUs |
            | Memory |

    Scenario: Verify Host Details table and its fields exists
        Given I am on the "Review" section
        Then I should see "Host Details" heading
        And I should see the following columns in the table
            | fields |
            | Hostname |
            | Labels |
            | CPUs |
            | Cores |
            | Total Memory |
            | Raw Capacity |
            | HDDs |
            | Flash |
            | NICs |
