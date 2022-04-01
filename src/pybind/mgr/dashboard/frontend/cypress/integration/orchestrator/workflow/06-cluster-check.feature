Feature: Cluster expansion verification and cluster check

    Go to hosts page and check whether the basic Daemons
    required for the cluster to become healthy are present inorder to verify
    that the cluster expansion process went smoothly as expected

    Background: Log in
        Given I am logged in

    Scenario: Cluster expansion wizard
        Given I am on the "welcome" page
        And I click on "Expand Cluster" button
        And I go to the "Review" section
        Then I click on "Expand Cluster" button
        Then I should be on the "dashboard" page

    Scenario: Add one more host
        Given I am on the "hosts" page
        When I click on "Add" button
        And enter "hostname" "ceph-node-03"
        And I click on submit button
        Then I should see a row with "ceph-node-03"

    Scenario Outline:  Check hosts and daemons are created
        Given I am on the "hosts" page
        Then I should see row "<hostname>" does not have "_no_schedule"
        When I expand the row "<hostname>"
        And I go to the "Daemons" tab
        Then I should see row "mon" have "running" on this tab

        Examples:
            | hostname |
            | ceph-node-00 |
            | ceph-node-01 |
            | ceph-node-02 |
            | ceph-node-03 |
