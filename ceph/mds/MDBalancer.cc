
#include "MDBalancer.h"
#include "MDS.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"

#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "include/Messenger.h"

#include <vector>
using namespace std;



int MDBalancer::proc_message(Message *m)
{
  switch (m->get_type()) {
	
  default:
	cout << "mds" << mds->get_nodeid() << " balancer unknown message " << m->get_type() << endl;
	throw "asdf";
	break;
  }

  return 0;
}


