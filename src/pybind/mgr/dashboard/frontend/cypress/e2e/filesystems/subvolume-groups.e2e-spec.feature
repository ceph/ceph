Feature: CephFS Subvolume Group management

    Goal: To test out the CephFS subvolume group management features

    Background: Login
        Given I am logged in

    Scenario: Create a CephFS Volume
        Given I am on the "cephfs" page
        And I click on "Create" button
        And enter "name" "test_cephfs"
        And I click on "Create File System" button
        Then I should see a row with "test_cephfs"

    Scenario: Create a CephFS Subvolume Group
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolume groups" tab
        And I click on "Create" button from the expanded row
        And enter "subvolumegroupName" "test_subvolume_group" in the modal
        And I click on "Create Subvolume group" button
        Then I should see a row with "test_subvolume_group" in the expanded row

    Scenario: Edit a CephFS Subvolume Group
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolume groups" tab
        When I select a row "test_subvolume_group" in the expanded row
        And I click on "Edit" button from the table actions in the expanded row
        And enter "size" "1" in the modal
        And I click on "Edit Subvolume group" button
        Then I should see row "test_subvolume_group" of the expanded row to have a usage bar

    Scenario: Remove a CephFS Subvolume Group
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolume groups" tab
        When I select a row "test_subvolume_group" in the expanded row
        And I click on "Remove" button from the table actions in the expanded row
        And I check the tick box in modal
        And I click on "Remove subvolume group" button
        Then I should not see a row with "test_subvolume_group" in the expanded row

    Scenario: Remove CephFS Volume
        Given I am on the "cephfs" page
        And I select a row "test_cephfs"
        And I click on "Remove" button from the table actions
        Then I should see the modal
        And I check the tick box in modal
        And I click on "Remove File System" button
        Then I should not see a row with "test_cephfs_edit"
