

#include <iostream>
#include <string>

#include "include/mds.h"
#include "include/dcache.h"


using namespace std;


DentryCache *readfiles() {
  DentryCache *dc = new DentryCache(new CInode());

  string fn;
  while (getline(cin, fn)) {
	CInode *in = new CInode();
	dc->add_file(fn, in);
	cout << "added " << fn << endl;
  }
  
  return dc;
}

int main(char **argv, int argc) {
  cout << "hi there" << endl;

  DentryCache *dc = readfiles();
  dc->get_root()->dump();

  return 0;
}

