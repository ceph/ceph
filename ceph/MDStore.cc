
#include "include/MDStore.h"
#include "assert.h"
#include "include/osd.h"
#include "include/mds.h"

#include <iostream>
using namespace std;



// == fetch_dir

class MDFetchDirContext : public Context {
 protected:
  MDStore *ms;
  CInode *in;
  Context *c;

 public:
  char *buf;
  size_t buflen;

  MDFetchDirContext(MDStore *ms, CInode *in, Context *c) : Context() {
	this->ms = ms;
	this->in = in;
	this->c = c;
	buf = 0;
	buflen = 0;
  }
  
  void finish(int result) {
	ms->fetch_dir_2( result, buf, buflen, in, c );
  }
};


bool MDStore::fetch_dir( CInode *in,
						 Context *c )
{
  // create return context
  MDFetchDirContext *fin = new MDFetchDirContext( this, in, c );

#if 1
  // issue osd read
  int osd = in->inode.ino % 10;
  object_t oid = in->inode.ino;
  
  osd_read( osd, oid, 
			0, 0, 
			(void**)&fin->buf,
			&fin->buflen,
			fin );
#else
  // fake dir read
  

#endif
}

bool MDStore::fetch_dir_2( int result, char *buf, size_t buflen, CInode *dir, Context *c )
{
  cout << "fetch_dir_2" << endl;

  // parse buffer contents into cache
  size_t p = 0;
  while (p < buflen) {
	// dentry
	string dname = buf+p;
	p += dname.length() + 1;
	cout << "parse filename " << dname << endl;

	// just a hard link?
	if (*(buf+p) == 'L') {
	  // yup.  we don't do that yet.
	  throw "not implemented";
	} else {
	  // inode
	  inodeno_t ino = ((struct inode_t*)(buf+p+1))->ino;
	  if (g_mds->mdcache->have_inode(ino)) 
		throw "inode already exists!  uh oh\n";

	  // new inode
	  CInode *in = new CInode();
	  memcpy(&in->inode, buf+p+1, sizeof(inode_t));
		
	  // add and link
	  g_mds->mdcache->add_inode( in );
	  g_mds->mdcache->link_inode( dir, dname, in );
	}
  }

  // ok!
  c->finish(0);
}


bool MDStore::commit_dir( CInode *in,
						  Context *c )
{
  

}
