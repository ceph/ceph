.. _hardware-monitoring:

===================
Hardware Monitoring
===================

``node-proxy`` is the internal name of the agent which inventories a machine's
hardware, provides different statuses, and enables the operator to perform
some actions.
It gathers details from the Redfish API that is often provided by an
out-of-band management interface present on server systems. The data is then
processed and pushed to an agent endpoint in the Ceph Manager.

.. graphviz::

     digraph G {
         node [shape=record];
         mgr [label="{<mgr> ceph manager}"];
         dashboard [label="<dashboard> ceph dashboard"];
         agent [label="<agent> agent"];
         redfish [label="<redfish> redfish"];
     
         agent -> redfish [label=" 1." color=green];
         agent -> mgr [label=" 2." color=orange];
         dashboard:dashboard -> mgr [label=" 3." color=lightgreen];
         node [shape=plaintext];
         legend [label=<<table border="0" cellborder="1" cellspacing="0">
             <tr><td bgcolor="lightgrey">Legend</td></tr>
             <tr><td align="center">1. Collects data from Redfish API</td></tr>
             <tr><td align="left">2. Pushes data to Ceph Manager</td></tr>
             <tr><td align="left">3. Queries Ceph Manager</td></tr>
         </table>>];
     }


Limitations
===========

For the time being, the ``node-proxy`` agent relies on the Redfish API.
This implies that both ``node-proxy`` agent and the Ceph Manager need to be
able to access the out-of-band network.


Deploying the Agent
===================

The first step is to provide the out-of-band management tool IP address and
credentials. This can be done when adding the host with a :ref:`service
spec <orchestrator-cli-service-spec>` file:

.. prompt:: bash # auto

  # cat host.yml
  ---
  service_type: host
  hostname: node-10
  addr: 10.10.10.10
  oob:
    addr: 20.20.20.10
    username: admin
    password: p@ssword

Apply the spec:

.. prompt:: bash # auto

  # ceph orch apply -i host.yml
  Added host 'node-10' with addr '10.10.10.10'

Deploy the agent:

.. prompt:: bash # auto

  # ceph config set mgr mgr/cephadm/hw_monitoring true


CLI
===

| **orch** **hardware** **status** [hostname] [--category CATEGORY] [--format plain | json]

Supported categories are:

* summary (default)
* memory
* storage
* processors
* network
* power
* fans
* temperatures
* firmware
* criticals


Examples
--------


Hardware Health Status Summary
______________________________

.. prompt:: bash # auto

  # ceph orch hardware status
  +---------+---------+---------+-----+-----+--------+-------+------+
  |   HOST  |    SN   | STORAGE | CPU | NET | MEMORY | POWER | FANS |
  +---------+---------+---------+-----+-----+--------+-------+------+
  | node-10 | FR8Y5X3 |    ok   |  ok |  ok |   ok   |   ok  |  ok  |
  +---------+---------+---------+-----+-----+--------+-------+------+


Memory Report
_____________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category memory
  +---------+--------+---------+--------+---------+
  |   HOST  | SYS_ID |   NAME  | STATUS |  STATE  |
  +---------+--------+---------+--------+---------+
  | node-10 |   1    | DIMM A1 |   OK   | Enabled |
  | node-10 |   1    | DIMM A2 |   OK   | Enabled |
  | node-10 |   1    | DIMM B1 |   OK   | Enabled |
  | node-10 |   1    | DIMM B2 |   OK   | Enabled |
  +---------+--------+---------+--------+---------+


Storage Devices Report
______________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category storage
  +---------+--------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+--------+---------+
  |   HOST  | SYS_ID |                          NAME                         |      MODEL       |      SIZE      | PROTOCOL |       SN       | SLOT | FW | STATUS |  STATE  |
  +---------+--------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+--------+---------+
  | node-10 |   1    | Disk 8 in Backplane 1 of Storage Controller in Slot 2 | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99QLL    |  8   |    |   OK   | Enabled |
  | node-10 |   1    | Disk 0 in Backplane 0 of Storage Controller in Slot 2 | MZ7L33T8HBNAAD3  | 3840755981824  |   SATA   | S6M5NE0T800539 |  0   |    |   OK   | Enabled |
  +---------+--------+-------------------------------------------------------+------------------+----------------+----------+----------------+------+----+--------+---------+


Processors Report
_________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category processors
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+
  |   HOST  | SYS_ID |     NAME     |                   MODEL                    | CORES | THREADS | STATUS |  STATE  |
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+
  | node-10 |   1    | cpu.socket.1 | Intel(R) Xeon(R) Silver 4314 CPU @ 2.40GHz |   16  |    32   |   OK   | Enabled |
  | node-10 |   1    | cpu.socket.2 | Intel(R) Xeon(R) Silver 4314 CPU @ 2.40GHz |   16  |    32   |   OK   | Enabled |
  +---------+--------+--------------+--------------------------------------------+-------+---------+--------+---------+


Network Devices Report
______________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category network
  +---------+--------+----------------------------------+-------+--------+---------+
  |   HOST  | SYS_ID |               NAME               | SPEED | STATUS |  STATE  |
  +---------+--------+----------------------------------+-------+--------+---------+
  | node-10 |   1    | NIC in Slot 1 Port 1 Partition 1 | 10000 |   OK   | Enabled |
  | node-10 |   1    |             eno8303              |  1000 |   OK   | Enabled |
  +---------+--------+----------------------------------+-------+--------+---------+


Power Supplies Report
_____________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category power
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |    NAME    |          MODEL          | MANUFACTURER | STATUS |  STATE  |
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+
  | node-10 |     1      | 0  | PS1 Status | PWR SPLY,800W,RDNT,LTON |     DELL     |   OK   | Enabled |
  | node-10 |     1      | 1  | PS2 Status | PWR SPLY,800W,RDNT,LTON |     DELL     |   OK   | Enabled |
  +---------+------------+----+------------+-------------------------+--------------+--------+---------+


Fans Report
___________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category fans
  +---------+------------+----+--------------------+---------+-------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |        NAME        | READING | UNITS | STATUS |  STATE  |
  +---------+------------+----+--------------------+---------+-------+--------+---------+
  | node-10 |     1      | 0  | System Board Fan1A |   4800  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 1  | System Board Fan1B |   4680  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 2  | System Board Fan2A |   4920  |  RPM  |   OK   | Enabled |
  | node-10 |     1      | 3  | System Board Fan2B |   4560  |  RPM  |   OK   | Enabled |
  +---------+------------+----+--------------------+---------+-------+--------+---------+


Temperature Sensors Report
__________________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category temperatures
  +---------+------------+----+---------------------------+---------+-------+--------+---------+
  |   HOST  | CHASSIS_ID | ID |            NAME           | READING | UNITS | STATUS |  STATE  |
  +---------+------------+----+---------------------------+---------+-------+--------+---------+
  | node-10 |     1      | 0  |  System Board Inlet Temp  |    24   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 1  |         CPU1 Temp         |    47   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 2  |         CPU2 Temp         |    45   |  Cel  |   OK   | Enabled |
  | node-10 |     1      | 3  | System Board Exhaust Temp |    38   |  Cel  |   OK   | Enabled |
  +---------+------------+----+---------------------------+---------+-------+--------+---------+


Firmware Details
________________

.. prompt:: bash # auto

  # ceph orch hardware status node-10 --category firmware
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |    HOST    |                                 COMPONENT                                  |                             NAME                             |         DATE         |   VERSION   | STATUS |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |   node-10  |               current-107649-7.03__raid.backplane.firmware.0               |                         Backplane 0                          | 2022-12-05T00:00:00Z |     7.03    |   OK   |


  ... omitted output ...


  |   node-10  |               previous-25227-6.10.30.20__idrac.embedded.1-1                |             Integrated Remote Access Controller              |      00:00:00Z       |  6.10.30.20 |   OK   |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+


Hardware Critical Warnings Report
_________________________________

.. prompt:: bash # auto

  # ceph orch hardware status --category criticals
  +---------+--------+-----------+------------+----------+-----------+
  |   HOST  | SYS_ID | COMPONENT |    NAME    |  STATUS  |   STATE   |
  +---------+--------+-----------+------------+----------+-----------+
  | node-10 |   1    |   power   | PS2 Status | critical | unplugged |
  +---------+--------+-----------+------------+----------+-----------+


For Developers
==============

.. py:currentmodule:: cephadm.agent
.. autoclass:: NodeProxyEndpoint
.. automethod:: NodeProxyEndpoint.__init__
.. automethod:: NodeProxyEndpoint.oob
.. automethod:: NodeProxyEndpoint.data
.. automethod:: NodeProxyEndpoint.fullreport
.. automethod:: NodeProxyEndpoint.summary
.. automethod:: NodeProxyEndpoint.criticals
.. automethod:: NodeProxyEndpoint.memory
.. automethod:: NodeProxyEndpoint.storage
.. automethod:: NodeProxyEndpoint.network
.. automethod:: NodeProxyEndpoint.power
.. automethod:: NodeProxyEndpoint.processors
.. automethod:: NodeProxyEndpoint.fans
.. automethod:: NodeProxyEndpoint.temperatures
.. automethod:: NodeProxyEndpoint.firmware
.. automethod:: NodeProxyEndpoint.led

