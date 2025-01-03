Feature: CephFS Snapshot Management

    Goal: To test out the CephFS snapshot and clone management features

    Background: Login
        Given I am logged in

    Scenario: Create a CephFS Volume
        Given I am on the "cephfs" page
        And I click on "Create" button
        And enter "name" "test_cephfs"
        And I click on "Create File System" button
        Then I should see a row with "test_cephfs"

    Scenario: Snapshots tab without a subvolume
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Snapshots" tab
        Then I should see an alert "No subvolumes are present" in the expanded row

    Scenario: Create a CephFS Subvolume
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolumes" tab
        And I click on "Create" button from the expanded row
        And enter "subvolumeName" "test_subvolume" in the carbon modal
        And I click on "Create Subvolume" button
        Then I should see a row with "test_subvolume" in the expanded row

    Scenario: Show the CephFS Snapshots view
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Snapshots" tab
        Then I should see a table in the expanded row

    Scenario: Create a CephFS Subvolume Snapshot
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Snapshots" tab
        And I click on "Create" button from the expanded row
        And enter "snapshotName" "test_snapshot" in the carbon modal
        And I click on "Create Snapshot" button
        Then I should see a row with "test_snapshot" in the expanded row

    Scenario: Create a CephFS Subvolume Snapshot Clone
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Snapshots" tab
        And I select a row "test_snapshot" in the expanded row
        And I click on "Clone" button from the table actions in the expanded row
        And enter "cloneName" "test_clone" in the carbon modal
        And I click on "Create Clone" button
        Then I wait for "5" seconds
        And I go to the "Subvolumes" tab
        Then I should see a row with "test_clone" in the expanded row

    Scenario: Remove a CephFS Subvolume Snapshot Clone
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Subvolumes" tab
        And I select a row "test_clone" in the expanded row
        And I click on "Remove" button from the table actions in the expanded row
        And I check the tick box in carbon modal
        And I click on "Remove Subvolume" button
        Then I wait for "5" seconds
        And I should not see a row with "test_clone" in the expanded row

    Scenario: Remove a CephFS Subvolume Snapshot
        Given I am on the "cephfs" page
        When I expand the row "test_cephfs"
        And I go to the "Snapshots" tab
        And I select a row "test_snapshot" in the expanded row
        And I click on "Remove" button from the table actions in the expanded row
        And I check the tick box in carbon modal
        And I click on "Remove Snapshot" button
        Then I should not see a row with "test_snapshot" in the expanded row

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
        Then I should not see a row with "test_cephfs"
