
#include "OSDMonitor.h"
#include "MDS.h"
#include "osd/OSDCluster.h"

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
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".osdmon: "


void OSDMonitor::init_my_stuff()
{

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




