
#ifndef __MDS_H
#define __MDS_H


#include <sys/types.h>
#include <list>

#include "dcache.h"



// 

class CMDS {
 protected:
  int          nodeid;
  int          num_nodes;

  // cache
  DentryCache *dc;

  // import/export
  list<CInode*>      import_list;
  list<CInode*>      export_list;
  
  // message queues
 

  
 public:
  CMDS(int id, int num) {
	nodeid = id;
	num_nodes = num;
	dc = NULL;
  }
  ~CMDS() {
	if (dc) { delete dc; dc = NULL; }
  }
};





#endif
