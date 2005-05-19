#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

class MDS;
class Message;

class OSDMonitor {
  MDS *mds;

 public:
  OSDMonitor(MDS *mds) {
	this->mds = mds;
  }

  void proc_message(Message *m);
  void handle_ping(class MPing *m);

};

#endif
