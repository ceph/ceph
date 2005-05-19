
#include "OSDMonitor.h"

#include "msg/Message.h"
#include "messages/MPing.h"


/* to send a message,

Message *messageptr = ......;
mds->messenger->send_message(messageptr,
                             MSG_ADDR_OSD(osdnum), 0, MSG_PORT_OSDMON);

*/


void OSDMonitor::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_PING:
	handle_ping((MPing*)m);
	break;
  }
}


void OSDMonitor::handle_ping(MPing *m)
{

}


