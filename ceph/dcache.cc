
#include "include/mds.h"
#include "include/dcache.h"

#include <iostream>
using namespace std;

// ====== CInode =======


void CInode::add_parent(CDentry *p) {
  nparents++;
  if (nparents == 1)         // first
	parent = p;
  else if (nparents == 2) {  // second, switch to the vector
	parents.push_back(parent);
	parents.push_back(p);
  } else                     // additional
	parents.push_back(p);
}

void CInode::dump(int d)
{
  string ind(' ', d);
  cout << ind << "inode" << endl;
  
  if (dentries)
	dentries->dump(d);
}






// CDentry

void CDentry::remove() {
  dir->remove_child(this);
}

CDentry::CDentry(const CDentry& m) {
  throw 1; //std::cerr << "copy cons called, implement me" << endl;
}

// =
const CDentry& CDentry::operator= (const CDentry& right) {
  throw 1;//std::cerr << "copy op called, implement me" << endl;
}

  // comparisons
  bool CDentry::operator== (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator!= (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator< (const CDentry& right) const {
	return name < right.name;
  }
  bool CDentry::operator> (const CDentry& right) const {
	return name > right.name;
  }
  bool CDentry::operator>= (const CDentry& right) const {
	return name >= right.name;
  }
  bool CDentry::operator<= (const CDentry& right) const {
	return name <= right.name;
  }


// CDir

void CDir::add_child(CDentry *d) {
  nitems++;
  items[d->name] = d;
  d->dir = this;
  
  if (nitems == 1)
	inode->get();       // pin down parent
}

void CDir::remove_child(CDentry *d) {
  map<string, CDentry*>::iterator iter = items.find(d->name);
  items.erase(iter);
  nitems = items.size();
  
  if (nitems == 0)
	inode->put();       // release parent.
}

CDentry* CDir::lookup(string n) {
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter != items.end()) return NULL;
  return iter->second;
}


void CDir::dump(int depth) {
  string ind(' ',depth);

  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	cout << ind << " " << d->name << endl;
	d->inode->dump(depth+1);
	iter++;
  }

}
