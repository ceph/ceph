

#include <sys/stat.h>
#include <iostream>
#include <string>

#include "include/MDS.h"
#include "include/MDCache.h"
#include "include/MDStore.h"
#include "include/FakeMessenger.h"

#include "messages/MPing.h"

using namespace std;

__uint64_t ino = 1;


// this parses find output

DentryCache *readfiles() {
  DentryCache *dc = new DentryCache();

  string fn;
  int offs = -1;
  while (getline(cin, fn)) {
	string relfn;

	CInode *in = new CInode();

	if (offs < 0) {
	  if (fn == "/")
		offs = 0;
	  else 
		offs = fn.length();
	  //cout << "offs is " << offs << " from " << fn << endl;
	  relfn = "/";
	  in->inode.ino = 1;  // root
	} else 
	  relfn = fn.substr(offs);
	
	// fill out inode
	struct stat sb;
	stat(fn.c_str(), &sb);

	in->inode.ino = ino++; //sb.st_ino;
	in->inode.mode = sb.st_mode;
	in->inode.size = sb.st_size;
	in->inode.uid = sb.st_uid;
	in->inode.gid = sb.st_gid;
	in->inode.atime = sb.st_atime;
	in->inode.mtime = sb.st_mtime;
	in->inode.ctime = sb.st_ctime;
		
	// add
	dc->add_file(relfn, in);
	cout << "added " << relfn << endl;
  }
  
  return dc;
}

int main(char **argv, int argc) {
  cout << "hi there" << endl;

  // init
  MDS *mds1 = new MDS(0, 2, new FakeMessenger(MSG_ADDR_MDS(0)));
  mds1->open_root(NULL);
  mds1->init();

  mds1->mdstore->fetch_dir( mds1->mdcache->get_root(), NULL );

  
  MDS *mds2 = new MDS(1,2,new FakeMessenger(MSG_ADDR_MDS(1)));
  mds2->init();

  // send an initial message...?
  mds1->messenger->send_message(new MPing(10), 1);

  // loop
  fakemessenger_do_loop();


  // cleanup
  if (mds1->mdcache->clear()) {
	cout << "clean shutdown" << endl;
	mds1->mdcache->dump();
	delete mds1;
  } else {
	cout << "can't empty cache";
  }

  return 0;
}

