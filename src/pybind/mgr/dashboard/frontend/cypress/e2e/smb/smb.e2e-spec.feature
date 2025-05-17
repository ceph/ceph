Feature: SMB management

    Goal: Test SMB management; clusters, shares, and authentication resources join-auth (active directory) and users and groups (standalone)

    Background: Login
        Given I am logged in
        Given I am on the "smb" page

    Scenario: Create an Active Directory access resource from SMB cluster form
        And I click on "Create cluster" button
        And I click on "editDomainSettingsModal"
        And I click on "navigateCreateJoinSource" in the carbon modal
        Then I should be on the "create active-directory" page
        And enter "authId" "ad1"
        And enter "username" "admin"
        And enter "password" "password"
        # TODO change to "Create Active Directory user" due to https://github.com/ceph/ceph/pull/62636
        And I click on "Create Active directory (AD) access resource" button
        Then I should be on the "create cluster" page

    Scenario: Create an AD SMB cluster
        And I click on "Create cluster" button
        Then I should be on the "create cluster" page
        And enter "cluster_id" "adCluster"
        # select "auth_mode" "active-directory" is set by default
        And I click on "editDomainSettingsModal"
        And enter "realm" "realm_name" in the carbon modal
        And select "ref" "ad1" in the carbon modal
        And I click on "Update Active Directory (AD) parameters" button
        Then I should not see the carbon modal
        And I click on "Create Cluster" button
        Then I should see a row with "adCluster"

    Scenario: Edit an AD SMB cluster
        And I select a row "adCluster"
        When I click on "Edit" button from the table actions
        Then I should be on the "edit cluster adCluster" page
        And I click on "advanced-fieldset"
        And I click on "custom-dns"
        And enter "0" "0.0.0.0"
        And I click on "Edit Cluster" button
        Then I should see a row with "adCluster"

    Scenario: Create a standalone access resource
        And I go to the "Standalone" carbon tab
        And I click on "Create standalone" button
        Then I should be on the "create standalone" page
        And enter "usersGroupsId" "ug1"
        And enter "name" "admin"
        And enter "password" "password"
        # TODO change to "Standalone" due to https://github.com/ceph/ceph/pull/62636
        And I click on "Create Users and groups access resource" button
        Then I should see a row with "ug1"

    Scenario: Create a standalone SMB cluster
        And I click on "Create cluster" button
        Then I should be on the "create cluster" page
        And enter "cluster_id" "ugCluster"
        And select "auth_mode" "User"
        And select "select-0" "ug1"
        And I click on "Create Cluster" button
        Then I should see a row with "ugCluster"

    Scenario: Edit a standalone SMB cluster
        And I select a row "ugCluster"
        When I click on "Edit" button from the table actions
        Then I should be on the "edit cluster ugCluster" page
        And I click on "Add user group" button
        And select "select-1" "ug1"
        And I click on "Edit Cluster" button
        Then I should see a row with "ugCluster"

    Scenario: Create an SMB share
        Given a volume is available
        Given a subvolume group is available
        When I expand the row "adCluster"
        And I click on "Create" button
        Then I should be on the "create share" page
        And enter "share_id" "shareTest"
        And select "volume" "testFs"
        And select "subvolume_group" "testSubvGrp"
        And I click on "Create Share" button
        When I expand the row "adCluster"
        Then I should see a row with "shareTest" in the expanded row
