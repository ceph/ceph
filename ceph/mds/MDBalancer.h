#ifndef __MDBALANCER_H
#define __MDBALANCER_H

#include <ostream>
#include <list>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/types.h"
#include "common/Clock.h"

class MDS;
class Message;
class MHeartbeat;
class CInode;
class Context;
class CDir;

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

  void export_empties();
  void do_rebalance();
  void find_exports(CDir *dir, 
					double amount, 
					list<CDir*>& exports, 
					double& have);


  void hit_inode(class CInode *in);
  void hit_dir(class CDir *dir);
  void hit_recursive(class CDir *dir, timepair_t& now);

  void subtract_export(CDir *export);
  void add_import(CDir *import);

};


ostream& operator<<( ostream& out, mds_load_t& load );

#endif
