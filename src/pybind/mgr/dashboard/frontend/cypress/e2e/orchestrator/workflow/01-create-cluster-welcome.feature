Feature: Cluster expansion welcome screen

    Go to the welcome screen and decide whether
    to proceed to wizard or skips to landing page

    Background: Login
        Given I am logged in

    Scenario: Cluster expansion welcome screen
        Given I am on the "welcome" page
        And I should see a button to "Expand Cluster"
        And I should see a button to "Skip"
        And I should see a message "Please expand your cluster first"

    Scenario: Go to the Cluster expansion wizard
        Given I am on the "welcome" page
        And I should see a button to "Expand Cluster"
        When I click on "Expand Cluster" button
        Then I am on the "Add Hosts" section

    Scenario: Skips the process and go to the landing page
        Given I am on the "welcome" page
        And I should see a button to "Skip"
        When I click on "Skip" button
        And I confirm to "Continue"
        Then I should be on the "dashboard" page
