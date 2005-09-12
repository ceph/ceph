#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

#include <time.h>

#include <map>
#include <set>
using namespace std;

class MDS;
class Message;

class OSDMonitor {
  MDS *mds;

  map<int,time_t>  last_heard_from_osd;
  map<int,time_t>  last_pinged_osd; 
  // etc..

  set<int>         failed_osds;
  set<int>         my_osds;

 public:
  OSDMonitor(MDS *mds) {
	this->mds = mds;
  }

  void init();

  void proc_message(Message *m);
  void handle_ping_ack(class MPingAck *m);
  void handle_failure(class MFailure *m);

  // hack
  void fake_reorg();

};

#endif
