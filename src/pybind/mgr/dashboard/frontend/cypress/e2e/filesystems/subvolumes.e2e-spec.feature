Feature: CephFS Subvolume management

    Goal: To test out the CephFS subvolume management features

    Background: Login
        Given I am logged in

    Scenario: Create a CephFS Volume
        Given I am on the "cephfs" page
        And I click on "Create" button
        And enter "name" "test_cephfs"
        And I click on "Create File System" button
        Then I should see a row with "test_cephfs"

    Scenario: Create a CephFS Subvolume
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolumes" tab
        And I click on "Create" button from the expanded row
        And enter "subvolumeName" "test_subvolume" in the carbon modal
        And I click on "Create Subvolume" button
        Then I should see a row with "test_subvolume" in the expanded row

    Scenario: Edit a CephFS Subvolume
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolumes" tab
        When I select a row "test_subvolume" in the expanded row
        And I click on "Edit" button from the table actions in the expanded row
        And enter "size" "1" in the carbon modal
        And I click on "Edit Subvolume" button
        Then I should see row "test_subvolume" of the expanded row to have a usage bar

    Scenario: Remove a CephFS Subvolume
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolumes" tab
        When I select a row "test_subvolume" in the expanded row
        And I click on "Remove" button from the table actions in the expanded row
        And I check the tick box in carbon modal
        And I click on "Remove Subvolume" button
        Then I should not see a row with "test_subvolume" in the expanded row

    Scenario: Remove CephFS Volume
        Given I am on the "cephfs" page
        And I select a row "test_cephfs"
        And I click on "Remove" button from the table actions
        Then I should see the carbon modal
        And I check the tick box in carbon modal
        And I click on "Remove File System" button
        Then I should not see a row with "test_cephfs_edit"
