
#include "include/mds.h"
#include "include/dcache.h"

#include <errno.h>
#include <iostream>
#include <string>
using namespace std;

// ====== CInode =======

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
}

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

void CInode::dump(int dep)
{
  string ind(dep, '\t');
  //cout << ind << "[inode " << this << "]" << endl;
  
  if (dir)
	dir->dump(dep);
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
  //cout << "adding " << d->name << " to " << this << endl;
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
  //cout << " lookup " << n << " in " << this << endl;
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter == items.end()) return NULL;

  //cout << "  lookup got " << iter->second << endl;
  return iter->second;
}


void CDir::dump(int depth) {
  string ind(depth, '\t');

  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	char isdir = ' ';
	if (d->inode->dir != NULL) isdir = '/';
	cout << ind << d->inode->inode.ino << " " << d->name << isdir << endl;
	d->inode->dump(depth+1);
	iter++;
  }

  if (!complete) 
	cout << ind << "..." << endl;

}







// DentryCache

bool DentryCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  lru->lru_insert_mid(in);
  inode_map[ in->inode.ino ] = in;
}

bool DentryCache::remove_inode(CInode *o) 
{
  // detach from parents
  if (o->nparents == 1) {
	CDentry *dn = o->parent;
	dn->dir->remove_child(dn);
	delete dn;
  } 
  else if (o->nparents > 1) {
	throw "implement me";  
  }

  // remove from map
  inode_map.erase(o->inode.ino);

  return true;
}

bool DentryCache::trim(__int32_t max) {
  if (max < 0) 
	max = lru->lru_get_max();

  while (lru->lru_get_num() > max) {
	CInode *o = (CInode*)lru->lru_expire();
	if (!o) return false;

	remove_inode(o);
	delete o;
	
  }

  return true;
}


CInode* DentryCache::get_file(string& fn) {
  int off = 1;
  CInode *cur = root;
  
  // dirs
  while (off < fn.length()) {
	unsigned int slash = fn.find("/", off);
	if (slash == string::npos) 
	  slash = fn.length();	
	string n = fn.substr(off, slash-off);

	//cout << " looking up '" << n << "' in " << cur << endl;

	if (cur->dir == NULL) {
	  //cout << "   not a directory!" << endl;
	  return NULL;  // this isn't a directory.
	}

	CDentry* den = cur->dir->lookup(n);
	if (den == NULL) return NULL;   // file dne!
	cur = den->inode;
	off = slash+1;	
  }

  dump();
  lru->lru_status();

  return cur;  
}


int DentryCache::link_inode( CInode *parent, string& dname, CInode *in ) 
{
  if (!parent->dir) {
	return -ENOTDIR;  // not a dir
  }

  // create dentry
  CDentry* dn = new CDentry(dname, in);
  in->add_parent(dn);

  // add to dir
  parent->dir->add_child(dn);

  return 0;
}


void DentryCache::add_file(string& fn, CInode *in) {
  
  // root?
  if (fn == "/") {
	if (!root) 
	  root = in;
	//cout << " added root " << root << endl;
	return;
  }


  // file.
  int lastslash = fn.rfind("/");
  string dirpart = fn.substr(0,lastslash);
  string file = fn.substr(lastslash+1);

  //cout << "dirpart '" << dirpart << "' filepart '" << file << "' inode " << in << endl;
  
  CInode *idir = get_file(dirpart);
  if (idir == NULL) return;

  //cout << " got dir " << idir << endl;

  if (idir->dir == NULL) {
	//cerr << " making " << fn << " into a dir" << endl;
	idir->dir = new CDir(idir);
  }
  
  add_inode( in );
  link_inode( idir, file, in );

  // trim
  trim();

}
