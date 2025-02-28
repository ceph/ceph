.. _hardware-monitoring:

Hardware monitoring
===================

`node-proxy` is the internal name to designate the running agent which inventories a machine's hardware, provides the different statuses and enable the operator to perform some actions.
It gathers details from the RedFish API, processes and pushes data to agent endpoint in the Ceph manager daemon.

.. graphviz::

     digraph G {
         node [shape=record];
         mgr [label="{<mgr> ceph manager}"];
         dashboard [label="<dashboard> ceph dashboard"];
         agent [label="<agent> agent"];
         redfish [label="<redfish> redfish"];
     
         agent -> redfish [label=" 1." color=green];
         agent -> mgr [label=" 2." color=orange];
         dashboard:dashboard -> mgr [label=" 3."color=lightgreen];
         node [shape=plaintext];
         legend [label=<<table border="0" cellborder="1" cellspacing="0">
             <tr><td bgcolor="lightgrey">Legend</td></tr>
             <tr><td align="center">1. Collects data from redfish API</td></tr>
             <tr><td align="left">2. Pushes data to ceph mgr</td></tr>
             <tr><td align="left">3. Query ceph mgr</td></tr>
         </table>>];
     }


Limitations
-----------

For the time being, the `node-proxy` agent relies on the RedFish API.
It implies both `node-proxy` agent and `ceph-mgr` daemon need to be able to access the Out-Of-Band network to work.


Deploying the agent
-------------------

| The first step is to provide the out of band management tool credentials.
| This can be done when adding the host with a service spec file:

.. code-block:: bash

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

.. code-block:: bash

  # ceph orch apply -i host.yml
  Added host 'node-10' with addr '10.10.10.10'

Deploy the agent:

.. code-block:: bash

  # ceph config set mgr mgr/cephadm/hw_monitoring true

CLI
---

| **orch** **hardware** **status** [hostname] [--category CATEGORY] [--format plain | json]

supported categories are:

* summary (default)
* memory
* storage
* processors
* network
* power
* fans
* firmwares
* criticals

Examples
********


hardware health statuses summary 
++++++++++++++++++++++++++++++++

.. code-block:: bash

  # ceph orch hardware status
  +------------+---------+-----+-----+--------+-------+------+
  |    HOST    | STORAGE | CPU | NET | MEMORY | POWER | FANS |
  +------------+---------+-----+-----+--------+-------+------+
  |   node-10  |    ok   |  ok |  ok |   ok   |   ok  |  ok  |
  +------------+---------+-----+-----+--------+-------+------+


storage devices report
++++++++++++++++++++++

.. code-block:: bash

  # ceph orch hardware status IBM-Ceph-1 --category storage
  +------------+--------------------------------------------------------+------------------+----------------+----------+----------------+--------+---------+
  |    HOST    |                          NAME                          |      MODEL       |      SIZE      | PROTOCOL |       SN       | STATUS |  STATE  |
  +------------+--------------------------------------------------------+------------------+----------------+----------+----------------+--------+---------+
  |   node-10  | Disk 8 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99QLL    |   OK   | Enabled |
  |   node-10  | Disk 10 in Backplane 1 of Storage Controller in Slot 2 | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZYX    |   OK   | Enabled |
  |   node-10  | Disk 11 in Backplane 1 of Storage Controller in Slot 2 | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZWB    |   OK   | Enabled |
  |   node-10  | Disk 9 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZC9    |   OK   | Enabled |
  |   node-10  | Disk 3 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT9903Y    |   OK   | Enabled |
  |   node-10  | Disk 1 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT9901E    |   OK   | Enabled |
  |   node-10  | Disk 7 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZQJ    |   OK   | Enabled |
  |   node-10  | Disk 2 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99PA2    |   OK   | Enabled |
  |   node-10  | Disk 4 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99PFG    |   OK   | Enabled |
  |   node-10  | Disk 0 in Backplane 0 of Storage Controller in Slot 2  | MZ7L33T8HBNAAD3  | 3840755981824  |   SATA   | S6M5NE0T800539 |   OK   | Enabled |
  |   node-10  | Disk 1 in Backplane 0 of Storage Controller in Slot 2  | MZ7L33T8HBNAAD3  | 3840755981824  |   SATA   | S6M5NE0T800554 |   OK   | Enabled |
  |   node-10  | Disk 6 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZER    |   OK   | Enabled |
  |   node-10  | Disk 0 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT98ZEJ    |   OK   | Enabled |
  |   node-10  | Disk 5 in Backplane 1 of Storage Controller in Slot 2  | ST20000NM008D-3D | 20000588955136 |   SATA   |    ZVT99QMH    |   OK   | Enabled |
  |   node-10  |           Disk 0 on AHCI Controller in SL 6            |  MTFDDAV240TDU   |  240057409536  |   SATA   |  22373BB1E0F8  |   OK   | Enabled |
  |   node-10  |           Disk 1 on AHCI Controller in SL 6            |  MTFDDAV240TDU   |  240057409536  |   SATA   |  22373BB1E0D5  |   OK   | Enabled |
  +------------+--------------------------------------------------------+------------------+----------------+----------+----------------+--------+---------+



firmwares details
+++++++++++++++++

.. code-block:: bash

  # ceph orch hardware status node-10 --category firmwares
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |    HOST    |                                 COMPONENT                                  |                             NAME                             |         DATE         |   VERSION   | STATUS |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+
  |   node-10  |               current-107649-7.03__raid.backplane.firmware.0               |                         Backplane 0                          | 2022-12-05T00:00:00Z |     7.03    |   OK   |
  
  
  ... omitted output ...
  
  
  |   node-10  |               previous-25227-6.10.30.20__idrac.embedded.1-1                |             Integrated Remote Access Controller              |      00:00:00Z       |  6.10.30.20 |   OK   |
  +------------+----------------------------------------------------------------------------+--------------------------------------------------------------+----------------------+-------------+--------+


hardware critical warnings report
+++++++++++++++++++++++++++++++++

.. code-block:: bash

  # ceph orch hardware status --category criticals
  +------------+-----------+------------+----------+-----------------+
  |    HOST    | COMPONENT |    NAME    |  STATUS  |      STATE      |
  +------------+-----------+------------+----------+-----------------+
  |   node-10  |   power   | PS2 Status | critical |    unplugged    |
  +------------+-----------+------------+----------+-----------------+


Developpers
-----------

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
.. automethod:: NodeProxyEndpoint.firmwares
.. automethod:: NodeProxyEndpoint.led

