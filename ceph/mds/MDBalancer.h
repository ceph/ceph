#ifndef __MDBALANCER_H
#define __MDBALANCER_H

#include <ostream>
#include <ext/hash_map>
#include <list>
using namespace std;

#include "include/types.h"

class MDS;
class Message;
class MHeartbeat;
class CInode;
class Context;

class MDBalancer {
 protected:
  MDS *mds;
  
  int beat_epoch;

  hash_map<int, mds_load_t> mds_load;

 public:
  MDBalancer(MDS *m) {
	mds = m;
	beat_epoch = 0;
  }
  
  int proc_message(Message *m);
  
  void send_heartbeat();
  void handle_heartbeat(MHeartbeat *m);

  void do_rebalance();
  void find_exports(CInode *idir, 
					double amount, 
					list<CInode*>& exports, 
					double& have);

  void hit_inode(class CInode *in);
  void hit_dir(class CDir *dir);

};


ostream& operator<<( ostream& out, mds_load_t& load );

#endif
