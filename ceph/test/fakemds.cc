

#include <sys/stat.h>
#include <iostream>
#include <string>

#include "include/mds.h"
#include "include/MDCache.h"
#include "include/MDStore.h"
#include "include/FakeMessenger.h"

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
  g_mds = new MDS(0, 1, new FakeMessenger());
  g_mds->open_root(NULL);

  g_mds->mdstore->fetch_dir( g_mds->mdcache->get_root(), NULL );


  // send an initial message...?

  // loop
  fakemessenger_do_loop();


  // cleanup
  if (g_mds->mdcache->clear()) {
	cout << "clean shutdown" << endl;
	g_mds->mdcache->dump();
	delete g_mds;
  } else {
	cout << "can't empty cache";
  }

  return 0;
}

