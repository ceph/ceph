Feature: Cluster expansion host addition

    Add some hosts and perform some host related actions like editing the labels
    and removing the hosts from the cluster and verify all of the actions are performed
    as expected

    Background: Cluster expansion wizard
        Given I am logged in
        And I am on the "welcome" page
        And I click on "Expand Cluster" button

    Scenario Outline: Add hosts
        Given I am on the "Add Hosts" section
        When I click on "Add" button
        And enter "hostname" "<hostname>" in the modal
        And select options "<labels>"
        And I click on "Add Host" button
        Then I should not see the modal
        And I should see a row with "<hostname>"
        And I should see row "<hostname>" have "<labels>"

        Examples:
            | hostname | labels |
            | ceph-node-01 | mon, mgr |
            | ceph-node-02 ||

    Scenario Outline: Remove hosts
        Given I am on the "Add Hosts" section
        And I should see a row with "<hostname>"
        When I select a row "<hostname>"
        And I click on "Remove" button from the table actions
        Then I should see the modal
        And I check the tick box in modal
        And I click on "Remove Host" button
        Then I should not see the modal
        And I should not see a row with "<hostname>"

        Examples:
            | hostname |
            | ceph-node-01 |
            | ceph-node-02 |

    Scenario: Add hosts using pattern 'ceph-node-[01-02]'
        Given I am on the "Add Hosts" section
        When I click on "Add" button
        And enter "hostname" "ceph-node-[01-02]" in the modal
        And I click on "Add Host" button
        Then I should not see the modal
        And I should see rows with following entries
            | hostname |
            | ceph-node-01 |
            | ceph-node-02 |

    Scenario: Add existing host and verify it failed
        Given I am on the "Add Hosts" section
        And I should see a row with "ceph-node-00"
        When I click on "Add" button
        And enter "hostname" "ceph-node-00" in the modal
        Then I should see an error in "hostname" field

    Scenario Outline: Add and remove labels on host
        Given I am on the "Add Hosts" section
        When I select a row "<hostname>"
        And I click on "Edit" button from the table actions
        And "add" option "<labels>"
        And I click on "Edit Host" button
        Then I should see row "<hostname>" have "<labels>"
        When I select a row "<hostname>"
        And I click on "Edit" button from the table actions
        And "remove" option "<labels>"
        And I click on "Edit Host" button
        Then I should see row "<hostname>" does not have "<labels>"

        Examples:
            | hostname | labels |
            | ceph-node-01 | foo |
