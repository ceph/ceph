
#include "include/Messenger.h"
#include "include/mds.h"

#include "include/MDStore.h"


void Messenger::dispatch_message(Message *m)
{
  switch (m->get_subsys()) {
	
  case MSG_SUBSYS_MDSTORE:
	g_mds->mdstore->proc_message(m);
	break;
	
	/*
  case MSG_SUBSYS_MDLOG:
	g_mds->logger->proc_message(m);
	break;
	
  case MSG_SUBSYS_BALANCER:
	g_mds->balancer->proc_message(m);
	break;
	*/

  case MSG_SUBSYS_SERVER:
	g_mds->proc_message(m);
	break;
  }
}
