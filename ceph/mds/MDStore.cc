
#include "MDStore.h"
#include "MDS.h"

#include <iostream>
using namespace std;



// == fetch_dir


class MDFetchDirContext : public Context {
 protected:
  MDStore *ms;
  CInode *in;

 public:
  char *buf;
  size_t buflen;

  MDFetchDirContext(MDStore *ms, CInode *in) : Context() {
	this->ms = ms;
	this->in = in;
	buf = 0;
	buflen = 0;
  }
  ~MDFetchDirContext() {
	if (buf) { delete buf; buf = 0; }
  }
  
  void finish(int result) {
	ms->fetch_dir_2( result, buf, buflen, in );
  }
};


bool MDStore::fetch_dir( CInode *in,
						 Context *c )
{
  cout << "fetch_dir " << in->inode.ino << endl;
  if (c) 
	in->waiting_for_fetch.push_back(c);

  // already fetching?
  if (in->mid_fetch) {
	cout << "already fetching " << in->inode.ino << "; waiting" << endl;
	return true;
  }
  in->mid_fetch = true;

  // create return context
  MDFetchDirContext *fin = new MDFetchDirContext( this, in );

  // issue osd read
  int osd = in->inode.ino % 10;
  object_t oid = in->inode.ino;
  
  mds->osd_read( osd, oid, 
				 0, 0,
				 &fin->buf, &fin->buflen,
				 fin );
}

bool MDStore::fetch_dir_2( int result, char *buf, size_t buflen, CInode *dir)
{
  cout << "fetch_dir_2" << endl;

  // make sure we have a CDir
  if (dir->dir == NULL) {
	dir->dir = new CDir(dir);
  }

  // parse buffer contents into cache
  __uint32_t num = *((__uint32_t*)buf);
  size_t p = 4;
  while (p < buflen && num > 0) {
	// dentry
	string dname = buf+p;
	p += dname.length() + 1;
	//cout << "parse filename " << dname << endl;

	// just a hard link?
	if (*(buf+p) == 'L') {
	  // yup.  we don't do that yet.
	  throw "not implemented";
	} else {
	  p++;

	  // inode
	  CInode *in = new CInode();
	  memcpy(&in->inode, buf+p, sizeof(inode_t));
	  p += sizeof(inode_t);
	  
	  cout << " got " << in->inode.ino << " " << dname << " isdir " << in->inode.isdir << endl;
	  if (mds->mdcache->have_inode(in->inode.ino)) 
		throw "inode already exists!  uh oh\n";
		
	  // add and link
	  mds->mdcache->add_inode( in );
	  mds->mdcache->link_inode( dir, dname, in );
	}
	num--;
  }
  
  dir->dir->state_set(CDIR_MASK_COMPLETE);

  // finish
  list<Context*> finished = dir->waiting_for_fetch;
  dir->waiting_for_fetch.clear();
  dir->mid_fetch = false;

  list<Context*>::iterator it = finished.begin();	
  while (it != finished.end()) {
	Context *c = *(it++);
	if (c) {
	  c->finish(0);
	  delete c;
	}
  }
}








// ----------------------------
// commit_dir

class MDCommitDirContext : public Context {
 protected:
  MDStore *ms;
  CInode *in;
  Context *c;
  __uint64_t version;

 public:
  char *buf;
  size_t buflen;

  MDCommitDirContext(MDStore *ms, CInode *in, Context *c) : Context() {
	this->ms = ms;
	this->in = in;
	this->c = c;
	buf = 0;
	buflen = 0;
	version = in->dir->get_version();
  }
  MDCommitDirContext() {
	if (buf) { delete buf; buf = 0; }
  }
  
  void finish(int result) {
	ms->commit_dir_2( result, in, c, version );
  }
};


class MDFetchForCommitContext : public Context {
protected:
  MDStore *ms;
  CInode *in;
  Context *co;
public:
  MDFetchForCommitContext(MDStore *m, CInode *i, Context *c) {
	ms = m; in = i; co = c;
  }
  void finish(int result) {
	ms->commit_dir( in, co );
  }
};



bool MDStore::commit_dir( CInode *in,
						  Context *c )
{
  // already committing?
  if (in->dir->get_state() & CDIR_MASK_MID_COMMIT) {
	// already mid-commit!
	cout << "dir already mid-commit" << endl;
	return false;
  }

  // is it complete?
  if (in->dir->get_state() & CDIR_MASK_COMPLETE == 0) {
	cout << "dir not complete, fetching first" << endl;
	// fetch dir first
	Context *fin = new MDFetchForCommitContext(this, in, c);
	fetch_dir(in, fin);
	return false;
  }

  // get continuation ready
  MDCommitDirContext *fin = new MDCommitDirContext(this, in, c);
  
  // buffer
  fin->buflen = in->dir->serial_size();
  fin->buf = new char[fin->buflen];
  __uint32_t num = 0;
  size_t off = sizeof(num);

  // fill
  CDir_map_t::iterator it = in->dir->begin();
  while (it != in->dir->end()) {
	// name
	memcpy(fin->buf + off, it->first.c_str(), it->first.length() + 1);
	off += it->first.length() + 1;
	
	// marker
	fin->buf[off++] = 'I';  // inode

	// inode
	memcpy(fin->buf + off, &it->second->get_inode()->inode, sizeof(inode_t));
	off += sizeof(inode_t);

	it++;	
	num++;
  }
  *((__uint32_t*)fin->buf) = num;
  
  // pin inode
  in->get();
  in->dir->state_set(CDIR_MASK_MID_COMMIT);

  // submit to osd
  int osd = in->inode.ino % 10;
  object_t oid = in->inode.ino;
  
  mds->osd_write( osd, oid, 
				  off, 0,
				  fin->buf,
				  0,
				  fin );
  return true;
}


bool MDStore::commit_dir_2( int result,
							CInode *in,
							Context *c,
							__uint64_t committed_version)
{
  // is the dir now clean?
  if (committed_version == in->dir->get_version()) {
	in->dir->state_clear(CDIR_MASK_DIRTY);   // clear dirty bit
  }
  in->dir->state_clear(CDIR_MASK_MID_COMMIT);

  // unpin inode
  in->put();

  // finish
  if (c) {
	c->finish(result);
	delete c;
  }
}
