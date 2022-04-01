Feature: Cluster expansion osd creation

    Create three osds so that the cluster will be in a
    healthy state when the cluster expansion process is
    finished

    Background: Cluster expansion wizard
        Given I am logged in
        And I am on the "welcome" page
        And I click on "Expand Cluster" button

    Scenario Outline: Create OSDs
        Given I am on the "Create OSDs" section
        When I click on "Add" button
        And I filter "Hostname" by "<hostname>"
        And I filter "Type" by "hdd"
        Then I click on "Add" button inside the modal
        Then I go to the "Review" section
        And I click on "Expand Cluster" button

        Examples:
            | hostname |
            | ceph-node-00 |
            | ceph-node-01 |
            | ceph-node-02 |
