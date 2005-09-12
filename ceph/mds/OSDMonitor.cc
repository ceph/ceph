
#include "OSDMonitor.h"
#include "MDS.h"
#include "osd/OSDMap.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".osdmon "



class C_OM_Faker : public Context {
public:
  OSDMonitor *om;
  C_OM_Faker(OSDMonitor *m) { 
	this->om = m;
  }
  void finish(int r) {
	om->fake_reorg();
  }
};


void OSDMonitor::fake_reorg() 
{
  
  // HACK osd map change
  dout(1) << "changing OSD map, removing one OSD" << endl;
  mds->osdmap->get_group(0).num_osds--;
  mds->osdmap->init_rush();
  mds->osdmap->inc_version();
  
  // bcast
  mds->bcast_osd_map();
    
  
}


void OSDMonitor::init()
{
  
  if (mds->get_nodeid() == 0 &&
	  mds->osdmap->get_group(0).num_osds > 4 &&
	  g_conf.fake_osdmap_expand) {
	dout(1) << "scheduling OSD map reorg at " << g_conf.fake_osdmap_expand << endl;
	g_timer.add_event_after(g_conf.fake_osdmap_expand,
							new C_OM_Faker(this));
  }
  
}


void OSDMonitor::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_FAILURE:
	handle_failure((MFailure*)m);
	break;

  case MSG_PING_ACK:
	handle_ping_ack((MPingAck*)m);
	break;
  }
}


void OSDMonitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}

void OSDMonitor::handle_failure(MFailure *m)
{
  dout(1) << "osd failure: " << MSG_ADDR_NICE(m->get_failed()) << " from " << MSG_ADDR_NICE(m->get_source()) << endl;
  
  // ack
  mds->messenger->send_message(new MFailureAck(m),
							   m->get_source(), m->get_source_port());
  delete m;
}




