
#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "MDStore.h"
#include "MDS.h"


// CDir

void CDir::add_child(CDentry *d) {
  //cout << "adding " << d->name << " to " << this << endl;
  items[d->name] = d;
  d->dir = this;
  
  nitems++;
  namesize += d->name.length();
  
  if (nitems == 1)
	inode->get();       // pin parent
}

void CDir::remove_child(CDentry *d) {
  map<string, CDentry*>::iterator iter = items.find(d->name);
  items.erase(iter);

  nitems--;
  namesize -= d->name.length();

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

  if (!(state & CDIR_MASK_COMPLETE))
	cout << ind << "..." << endl;
  if (state & CDIR_MASK_DIRTY)
	cout << ind << "[dirty]" << endl;

}


void CDir::dump_to_disk(MDS *mds)
{
  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	if (d->inode->dir != NULL) {
	  cout << "dump2disk: " << d->inode->inode.ino << " " << d->name << '/' << endl;
	  d->inode->dump_to_disk(mds);
	}
	iter++;
  }

  cout << "dump2disk: writing dir " << inode->inode.ino << endl;
  mds->mdstore->commit_dir(inode, NULL);
}
