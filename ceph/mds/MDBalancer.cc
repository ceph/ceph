
#include "MDBalancer.h"
#include "MDS.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"

#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "include/Messenger.h"

#include <vector>
using namespace std;


// basics

void MDBalancer::export_dir(CInode *in,
							int dest)
{
  if (!in->dir) in->dir = new CDir(in);

  string path;
  in->make_path(path);
  cout << "mds" << mds->get_nodeid() << " export_dir " << path << " to " << dest << endl;

  // freeze
  in->dir->freeze();

  in->dir->dir_dist = dest;

  MExportDir *req = new MExportDir(in);
  
  mds->messenger->send_message(req,
							   MSG_ADDR_MDS(dest), MDS_PORT_SERVER,
							   MDS_PORT_SERVER);
}


void MDBalancer::export_dir_ack(MExportDirAck *m)
{
  // exported!
  CInode *in = mds->mdcache->get_inode(m->ino);
  string path;
  in->make_path(path);

  cout << "mds" << mds->get_nodeid() << " export_dir_ack " << path << endl;
  
  in->get();   // pin it

  // remove the metadata from the cache
  // ...

  // unfreeze
  in->dir->unfreeze();
}

void MDBalancer::import_dir(MExportDir *m)
{
  cout << "mds" << mds->get_nodeid() << " import_dir " << m->path << endl;

  vector<CInode*> trav;
  vector<string>  trav_dn;
  
  int r = mds->path_traverse(m->path, trav, trav_dn, m, MDS_TRAVERSE_DISCOVER);   // FIXME BUG
  if (r > 0)
	return;  // did something

  CInode *in = trav[trav.size()-1];
  
  if (!in->dir) in->dir = new CDir(in);

  // it's mine, now!
  in->dir->dir_dist = mds->get_nodeid();


  // add this crap to my cache

  
  // send ack
  cout << "mds" << mds->get_nodeid() << " sending ack back to " << m->get_source() << endl;
  MExportDirAck *ack = new MExportDirAck(m);
  mds->messenger->send_message(ack,
							   m->get_source(), MDS_PORT_SERVER,
							   MDS_PORT_SERVER);
}


