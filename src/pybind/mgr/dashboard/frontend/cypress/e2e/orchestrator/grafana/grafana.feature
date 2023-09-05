Feature: Grafana panels

    Go to some of the grafana performance section and check if
    panels are populated without any issues

    Background: Log in
        Given I am logged in

    Scenario Outline: Hosts Overall Performance
        Given I am on the "hosts" page
        When I go to the "Overall Performance" tab
        Then I should see the grafana panel "<panel>"
        When I view the grafana panel "<panel>"
        Then I should not see "No Data" in the panel "<panel>"

        Examples:
            | panel |
            | OSD Hosts |
            | AVG CPU Busy |
            | AVG RAM Utilization |
            | Physical IOPS |
            | AVG Disk Utilization |
            | Network Load |
            | CPU Busy - Top 10 Hosts |
            | Network Load - Top 10 Hosts |

    Scenario Outline: RGW Daemon Overall Performance
        Given I am on the "rgw daemons" page
        When I go to the "Overall Performance" tab
        Then I should see the grafana panel "<panel>"
        When I view the grafana panel "<panel>"
        Then I should not see No Data in the graph "<panel>"
        And I should see the legends "<legends>" in the graph "<panel>"

        Examples:
            | panel | legends |
            | Total Requests/sec by RGW Instance | foo.ceph-node-00, foo.ceph-node-01, foo.ceph-node-02 |
            | GET Latencies by RGW Instance | foo.ceph-node-00, foo.ceph-node-01, foo.ceph-node-02 |
            | Bandwidth by RGW Instance | foo.ceph-node-00, foo.ceph-node-01, foo.ceph-node-02 |
            | PUT Latencies by RGW Instance | foo.ceph-node-00, foo.ceph-node-01, foo.ceph-node-02 |
            | Average GET/PUT Latencies by RGW Instance | GET, PUT |
            | Bandwidth Consumed by Type | GETs, PUTs |

    Scenario Outline: RGW per Daemon Performance
        Given I am on the "rgw daemons" page
        When I expand the row "<name>"
        And I go to the "Performance Details" tab
        Then I should see the grafana panel "<panel>"
        When I view the grafana panel "<panel>"
        Then I should not see No Data in the graph "<panel>"
        And I should see the legends "<name>" in the graph "<panel>"

        Examples:
            | name | panel |
            | foo.ceph-node-00 | Bandwidth by HTTP Operation |
            | foo.ceph-node-00 | HTTP Request Breakdown |
            | foo.ceph-node-01 | Bandwidth by HTTP Operation |
            | foo.ceph-node-01 | HTTP Request Breakdown |
            | foo.ceph-node-02 | Bandwidth by HTTP Operation |
            | foo.ceph-node-02 | HTTP Request Breakdown |
