
#include <list>

#include "include/mds.h"
#include "include/Messenger.h"
#include "include/MDCache.h"
#include "include/MDStore.h"

#include <iostream>
using namespace std;

// extern 
MDS *g_mds;


// cons/des
MDS::MDS(int id, int num) {
  nodeid = id;
  num_nodes = num;
  
  mdcache = new DentryCache();
  mdstore = new MDStore();
  messenger = new Messenger();
}
MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (messenger) { delete messenger; messenger = NULL; }
}


void MDS::proc_message(Message *m) 
{
  cout << "implement MDS::proc_message" << endl;
}



// ---------------------------
// open_root

class OpenRootContext : public Context {
protected:
  Context *c;
public:
  OpenRootContext(Context *c) {
	this->c = c;
  }
  void finish(int result) {
	g_mds->open_root_2(result, c);
  }
};

bool MDS::open_root(Context *c)
{
  // open root inode
  if (nodeid == 0) { 
	// i am root
	CInode *root = new CInode();
	root->inode.ino = 1;

	// make it up (FIXME)
	root->inode.mode = 0755;
	root->inode.size = 0;

	mdcache->set_root( root );

	if (c) {
	  c->finish(0);
	  delete c;
	}
  } else {
	// request inode from root mds
	

  }
}

bool MDS::open_root_2(int result, Context *c)
{
  c->finish(0);
  delete c;
}
