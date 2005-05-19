#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

#include <time.h>

#include <map>
using namespace std;

class MDS;
class Message;

class OSDMonitor {
  MDS *mds;

  map<int,time_t>  last_heard_from_osd;
  map<int,time_t>  last_pinged_osd;
  // etc..

 public:
  OSDMonitor(MDS *mds) {
	this->mds = mds;
  }

  void proc_message(Message *m);
  void handle_ping(class MPing *m);

};

#endif
