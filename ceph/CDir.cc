
#include "include/CDir.h"
#include "include/CDentry.h"
#include "include/CInode.h"
#include "include/mds.h"

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

  if (!complete) 
	cout << ind << "..." << endl;

}


void CDir::dump_to_disk()
{
  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	char isdir = ' ';
	if (d->inode->dir != NULL) isdir = '/';
	cout << "dump2disk: " << d->inode->inode.ino << " " << d->name << isdir << endl;
	d->inode->dump_to_disk();
	iter++;
  }

  g_mds->mdstore->commit_dir(inode, NULL);

}
