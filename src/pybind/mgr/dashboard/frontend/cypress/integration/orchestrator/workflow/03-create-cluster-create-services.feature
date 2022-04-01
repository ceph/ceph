Feature: Cluster expansion service creation

    Create some services and then verify that creating a service,
    editing a service and deleting a service works as expected.

    Background: Cluster expansion wizard
        Given I am logged in
        And I am on the "welcome" page
        And I click on "Expand Cluster" button

    Scenario: Create a service
        Given I am on the "Create Services" section
        Then I click on "Create" button
        And select "service_type" "mds"
        And enter "service_id" "test"
        And enter "count" "1"
        And I click on submit button
        Then I should see a row with "mds.test"

    Scenario: Edit a service
        Given I am on the "Create Services" section
        When I select a row "mds.test"
        And I click on "Edit" button from the table actions
        And enter "count" "2"
        And I click on submit button
        Then I should see row "mds.test" have "count:2"

    Scenario: Delete a service
        Given I am on the "Create Services" section
        When I select a row "mds.test"
        And I click on "Delete" button from the table actions
        Then I should see the modal
        And I check the tick box in modal
        And I click on "Delete Service" button
        Then I should not see the modal
        And I should not see a row with "mds.test"
