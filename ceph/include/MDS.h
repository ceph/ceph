
#ifndef __MDS_H
#define __MDS_H


#include <sys/types.h>
#include <list>

#include "MDCache.h"
#include "MDStore.h"
//#include "MDLog.h"
//#include "MDBalancer.h"


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
  //MDLog      *logger;
  //MDBalancer *balancer;
 
  Messenger  *messenger;



  
 public:
  MDS(int id, int num) {
	nodeid = id;
	num_nodes = num;

	mdcache = new DentryCache();
	mdstore = new MDStore();
  }
  ~MDS() {
	if (mdcache) { delete mdcache; mdcache = NULL; }
	if (mdstore) { delete mdstore; mdstore = NULL; }
  }

  void proc_message(Message *m);

};


extern MDS *g_mds;


#endif
