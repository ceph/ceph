

#include <iostream>
#include <string>

#include "include/mds.h"
#include "include/MDCache.h"


using namespace std;

__uint64_t ino = 1;


// this parses find output

DentryCache *readfiles() {
  DentryCache *dc = new DentryCache();
  dc->set_root(new CInode());

  string fn;
  int offs = -1;
  while (getline(cin, fn)) {
	string relfn;

	if (offs < 0) {
	  if (fn == "/")
		offs = 0;
	  else 
		offs = fn.length();
	  //cout << "offs is " << offs << " from " << fn << endl;
	  relfn = "/";
	} else 
	  relfn = fn.substr(offs);
	
	CInode *in = new CInode();
	in->inode.ino = ino++;
	dc->add_file(relfn, in);
	cout << "added " << relfn << endl;
  }
  
  return dc;
}

int main(char **argv, int argc) {
  cout << "hi there" << endl;

  g_mds = new MDS(0, 1);
  g_mds->mdcache = readfiles();
  
  g_mds->mdcache->dump();
  
  g_mds->mdcache->dump_to_disk();

  if (g_mds->mdcache->clear()) {
	cout << "clean shutdown" << endl;
	g_mds->mdcache->dump();
	delete g_mds;
  } else {
	throw "can't empty cache";
  }

  return 0;
}

