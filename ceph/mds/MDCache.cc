
#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"

#include <errno.h>
#include <iostream>
#include <string>
using namespace std;




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
  if (max < 0) {
	max = lru->lru_get_max();
	if (!max) return false;
  }

  while (lru->lru_get_size() > max) {
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

  //dump();
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
	root = in;
	add_inode( in );
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
	cerr << " making " << dirpart << " into a dir" << endl;
	idir->dir = new CDir(idir); 
	idir->inode.isdir = true;
  }
  
  add_inode( in );
  link_inode( idir, file, in );

  // trim
  trim();

}
