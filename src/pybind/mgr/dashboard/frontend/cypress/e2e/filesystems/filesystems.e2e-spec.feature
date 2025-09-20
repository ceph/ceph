Feature: CephFS Management

    Goal: To test out the CephFS management features

    Background: Login
        Given I am logged in

    Scenario: Create a CephFS Volume
        Given I am on the "cephfs" page
        And I click on "Create" button
        And enter "name" "test_cephfs"
        And I click on "Create File System" button
        Then I should see a row with "test_cephfs"

    # Should be uncommented once the pre-requisite is fixed
    # Scenario: Edit CephFS Volume
    #     Given I am on the "cephfs" page
    #     And I select a row "test_cephfs"
    #     And I click on "Edit" button
    #     And enter "name" "test_cephfs_edit"
    #     And I click on "Edit File System" button
    #     Then I should see a row with "test_cephfs_edit"

    Scenario: Remove CephFS Volume
        Given I am on the "cephfs" page
        And I select a row "test_cephfs"
        And I click on "Remove" button from the table actions
        Then I should see the carbon modal
        And I confirm the resource "test_cephfs"
        And I click on "Remove File System" button
        Then I should not see a row with "test_cephfs"

    Scenario Outline: Create two cephfs pools for attaching to a volume
        Given I am on the "create pool" page
        And enter "name" "<pool_name>"
        And select "poolType" "<type>"
        And select options "<application>"
        And I click on "Create Pool" button
        Then I should see a row with "<pool_name>"

        Examples:
            | pool_name | type | application |
            | e2e_cephfs_data | replicated | cephfs |
            | e2e_cephfs_meta | replicated | cephfs |

    Scenario Outline: Create a CephFS Volume with pre-created pools
        Given I am on the "create cephfs" page
        And enter "name" "<fs_name>"
        And checks "Use existing pools"
        And select "dataPool" "<data_pool>"
        And select "metadataPool" "<metadata_pool>"
        And I click on "Create File System" button
        Then I should see a row with "<fs_name>"

        Examples:
            | fs_name | data_pool | metadata_pool |
            | e2e_custom_pool_cephfs | e2e_cephfs_data | e2e_cephfs_meta |

    Scenario Outline: Remove CephFS Volume that has pre-created pools
        Given I am on the "cephfs" page
        And I select a row "<fs_name>"
        And I click on "Remove" button from the table actions
        Then I should see the carbon modal
        And I confirm the resource "<fs_name>"
        And I click on "Remove File System" button
        Then I should not see a row with "<fs_name>"

        # verify pools associated with the volume is removed
        Given I am on the "pools" page
        Then I should not see a row with "<data_pool>"
        And I should not see a row with "<metadata_pool>"

        Examples:
            | fs_name | data_pool | metadata_pool |
            | e2e_custom_pool_cephfs | e2e_cephfs_data | e2e_cephfs_meta |
