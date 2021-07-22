osd
===

.. graphviz::

   digraph osd {
     node [shape = doublecircle]; "start" "end";
     node [shape = circle];
     start -> preboot;
     waiting_for_healthy [label = "waiting\nfor\nhealthy"];
     waiting_for_healthy -> waiting_for_healthy [label = "tick"];
     waiting_for_healthy -> preboot [label = "i am healthy!"];
     preboot -> booting [label = "send(MOSDBoot)"];
     booting -> active [label = "recv(osdmap<up>)"];
     active -> prestop [label = "stop()"];
     active -> preboot [label = "recv(osdmap<down>)"];
     active -> end [label = "kill(SIGINT)"];
     active -> waiting_for_healthy [label = "i am unhealthy!"]
     prestop -> end [label = "recv(osdmap<down>)"];
   }

.. describe:: waiting_for_healthy

   If an OSD daemon is able to connected to its heartbeat peers, and its own
   internal hearbeat does not fail, it is considered healthy. Otherwise, it
   puts itself in the state of `waiting_for_healthy`, and check its own
   reachability and internal heartbeat periodically.

.. describe:: preboot

   OSD sends an `MOSDBoot` message to the connected monitor to inform the
   cluster that it's ready to serve, so that the quorum can mark it `up`
   in the osdmap.

.. describe:: booting

   Before being marked as `up`, an OSD has to stay in its `booting` state.

.. describe:: active

   Upon receiving an osdmap marking the OSD as `up`, it transits to `active`
   state. After that, it is entitled to do its business. But the OSD service
   can be fully stopped or suspended due to various reasons. For instance,
   the osd services can be stopped by administrator manually, or marked `stop`
   in the osdmap. Or any of its IP addresses does not match with the
   corresponding one configured in osdmap, it transits to `preboot` if
   it considers itself healthy.

.. describe:: prestop

   The OSD transits to `prestop` unconditionally upon request of `stop`.
   But before bidding us farewell, it tries to get the acknowledge from
   the monitor by sending an `MOSDMarkMeDown`, and waiting for an response
   of updated osdmap or another `MOSDMarkMeDown` message.
