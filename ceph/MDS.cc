
#include <list>

#include "include/mds.h"
#include "include/Messenger.h"
#include "include/MDCache.h"
#include "include/MDStore.h"

#include "messages/MPing.h"

#include <iostream>
using namespace std;

// extern 
//MDS *g_mds;


// cons/des
MDS::MDS(int id, int num, Messenger *m) {
  nodeid = id;
  num_nodes = num;
  
  mdcache = new DentryCache();
  mdstore = new MDStore(this);
  messenger = m;
}
MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (messenger) { delete messenger; messenger = NULL; }
}


int MDS::init()
{
  messenger->init(this);
}

void MDS::shutdown()
{
  messenger->shutdown();
}

void MDS::proc_message(Message *m) 
{
  switch (m->get_type()) {
  case MSG_PING:
	cout << nodeid << " received ping from " << m->get_from() << " with count " << ((MPing*)m)->num << endl;
	if (((MPing*)m)->num > 0) {
	  cout << nodeid << " responding to " << m->get_from() << endl;
	  messenger->send_message(new MPing(((MPing*)m)->num-1), m->get_from());
	}
	break;

  default:
	cout << "implement MDS::proc_message" << endl;
  }

}



// ---------------------------
// open_root

class OpenRootContext : public Context {
protected:
  Context *c;
  MDS *mds;
public:
  OpenRootContext(MDS *m, Context *c) {
	mds = m;
	this->c = c;
  }
  void finish(int result) {
	mds->open_root_2(result, c);
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
