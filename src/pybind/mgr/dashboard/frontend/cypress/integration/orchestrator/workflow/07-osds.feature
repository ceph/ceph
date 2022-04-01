Feature: OSD Page

    Go to the osd page and confirm that the osds
    created from the cluster expansion workflow is present.
    Atleast 3 osds should be present to say that
    cluster is healthy

    Background: Log in
        Given I am logged in

    Scenario Outline: Verify atleast 3 osds are created and its 'in' and 'up'
        Given I am on the "osds" page
        Then I should see row "<osd_id>" have "<status>"

        Examples:
            | osd_id | status |
            | 0 | in, up |
            | 1 | in, up |
            | 2 | in, up |
