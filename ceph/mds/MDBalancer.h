#ifndef __MDBALANCER_H
#define __MDBALANCER_H

class MDS;
class Message;

class MDBalancer {
 protected:
  MDS *mds;
  
 public:
  MDBalancer(MDS *m) {
	mds = m;
  }
  
  int proc_message(Message *m);
				  
};

#endif
