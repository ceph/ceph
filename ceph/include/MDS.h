
#ifndef __MDS_H
#define __MDS_H


#include <sys/types.h>
#include <list>

#include "Context.h"


class CInode;
class DentryCache;
class MDStore;
class MDLog;
class Messenger;
class Message;


using namespace std;

// 

class MDS {
 protected:
  int          nodeid;
  int          num_nodes;

  // import/export
  list<CInode*>      import_list;
  list<CInode*>      export_list;
  

  friend class MDStore;

 public:
  // sub systems
  DentryCache  *mdcache;    // cache
  MDStore      *mdstore;    // storage interface
  Messenger    *messenger;    // message processing

  MDLog        *logger;
  //MDBalancer   *balancer;
 




  
 public:
  MDS(int id, int num, Messenger *m);
  ~MDS();

  int get_nodeid() {
	return nodeid;
  }

  int init();
  void shutdown();

  void proc_message(Message *m);

  bool open_root(Context *c);
  bool open_root_2(int result, Context *c);

};


//extern MDS *g_mds;


#endif
