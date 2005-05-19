
#include "OSDMonitor.h"
#include "MDS.h"
#include "osd/OSDCluster.h"

#include "msg/Message.h"

#include "messages/MPing.h"

#include "common/Timer.h"
#include "common/Clock.h"

/* to send a message,

Message *messageptr = ......;
mds->messenger->send_message(messageptr,
                             MSG_ADDR_OSD(osdnum), 0, MDS_PORT_OSDMON);


timer example:

class C_Test : public Context {
  OSDMonitor *om;
public:
  C_Test(OSDMonitor *om) {
     this->om = om;
  }
  void finish(int r) {
	cout << "C_Test->finish(" << r << ")" << endl;
    om->check_for_ping_timeouts_or_something();
  }
};

g_timer.add_event_after(10, new C_Test);


to tell which mds we are, mds->get_nodeid()  (out of mds->get_cluster()->get_num_mds())

*/


void OSDMonitor::init_my_stuff()
{
  set<int> all_osds;
  mds->osdcluster->get_all_osds(all_osds);

  // pick mine
  for (set<int>::iterator it = all_osds.begin();
	   it != all_osds.end();
	   it++) {
	if (1 /* something */) my_osds.insert(*it);
  }
}


void OSDMonitor::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_PING:
	handle_ping((MPing*)m);
	break;
  }
}



// simple heartbeat for now

class C_CheckHeartbeat : public Context {
  OSDMonitor *om;
public:
  C_CheckHeartbeat(OSDMonitor *om) {
     this->om = om;
  }
  void finish(int r) {
    om->check_heartbeat();
  }
};

void OSDMonitor::initiate_heartbeat()
{
  // send out pings
  
  
  // set timer for 10s later
  g_timer.add_event_after(10, new C_CheckHeartbeat(this));
  
}

void OSDMonitor::check_heartbeat()
{
  // blah
  
  
}


void OSDMonitor::handle_ping(MPing *m)
{
  
  // check m->get_osd_status();
  


}




