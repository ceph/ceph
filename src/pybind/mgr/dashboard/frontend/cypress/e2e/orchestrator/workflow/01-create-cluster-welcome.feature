Feature: Onboarding welcome screen

    Go to the welcome onboarding screen and decide whether
    to proceed to wizard or skips to landing page

    Background: Login
        Given I am logged in

    Scenario: Onboarding welcome screen
        Given I am on the "onboarding" page
        And I should see a button to "Add Storage"
        And I should see a button to "View cluster overview"
        And I should see a message "Welcome to Ceph Dashboard"

    Scenario: Go to the Add storage wizard
        Given I am on the "onboarding" page
        And I should see a button to "Add Storage"
        When I click on "Add Storage" button
        Then I am on the "Add Hosts" section

    Scenario: Skips the process and go to the landing page
        Given I am on the "onboarding" page
        And I should see a button to "View cluster overview"
        When I click on "View cluster overview" button
        And I confirm to "Continue" on carbon modal
        Then I should be on the "overview" page
