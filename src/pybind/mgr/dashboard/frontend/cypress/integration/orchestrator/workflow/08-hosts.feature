Feature: Hosts Page

    Go to hosts page and then do
    some host actions like the force maintenance
    and host drain. Verify they are performed as intended.

    Background: Log in
        Given I am logged in

    Scenario: Create an rgw service. Its needed for a successful host maintenance action
        Given I am on the "services" page
        When I click on "Create" button
        And select "service_type" "mds"
        And enter "service_id" "foo"
        And enter "count" "4"
        And I click on submit button
        Then I should see a row with "rgw.foo"

    Scenario: Force a host to maintenance
        Given I am on the "hosts" page
        When I select a row "ceph-node-03"
        And I click on "Enter Maintenance" button from the table actions
        And I should see the modal
        And I click on "Continue" button
        Then I should see row "ceph-node-03" have "maintenance"
        When I click on "Exit Maintenance" button from the table actions
        Then I should see row "ceph-node-03" does not have "maintenance"

    Scenario: Drain and remove a host
        Given I am on the "hosts" page
        When I select a row "ceph-node-03"
        And I click on "Start Drain" button from the table actions
        Then I should see row "ceph-node-03" have "_no_schedule"
        Then I expand the row "ceph-node-03"
        And I go to the "Daemons" tab
        Then I should see the table is empty on this tab
        When I click on "Remove" button from the table actions
        And I should see the modal
        And I check the tick box in modal
        And I click on "Remove Host" button
        Then I should not see the modal
        And I should not see a row with "<hostname>"
