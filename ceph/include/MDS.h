
#ifndef __MDS_H
#define __MDS_H


#include <sys/types.h>
#include <list>

#include "dcache.h"
#include "MDStore.h"
//#include "MDLog.h"
//#include "MDBalancer.h"


// 

class MDS {
 protected:
  int          nodeid;
  int          num_nodes;

  // cache
  DentryCache *mdcache;

  // import/export
  list<CInode*>      import_list;
  list<CInode*>      export_list;
  

  // sub systems
  MDStore    *mdstore;
  //MDLog      *logger;
  //MDBalancer *balancer;
 
  Messenger  *messenger;


  friend class MDStore;
  
 public:
  MDS(int id, int num) {
	nodeid = id;
	num_nodes = num;
	mdcache = NULL;
  }
  ~MDS() {
	if (mdcache) { delete mdcache; mdcache = NULL; }
  }

  void proc_message(Message *m);

};


extern MDS *g_mds;


#endif
