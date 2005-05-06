#ifndef __CLIENT_H
#define __CLIENT_H

#include "mds/MDCluster.h"

#include "msg/Message.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "msg/SerialMessenger.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

//#include "msgthread.h"

#include "include/types.h"
#include "include/lru.h"
#include "include/filepath.h"

// stl
#include <set>
#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;



// ============================================
// types for my local metadata cache
/* basic structure:
   
 - Dentries live in an LRU loop.  they get expired based on last access.
      see include/lru.h.  items can be bumped to "mid" or "top" of list, etc.
 - Inode has ref count for each Fh, Dir, or Dentry that points to it.
 - when Inode ref goes to 0, it's expired.
 - when Dir is empty, it's removed (and it's Inode ref--)
 
*/

class Dentry;
class Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  hash_map< string, Dentry* > dentries;

  Dir(Inode* in) { parent_inode = in; }
};

class Inode {
 public:
  inode_t   inode;    // the actual inode
  set<int>	mds_contacts;
  time_t    last_updated;

  int       ref;      // ref count. 1 for each dentry, fh or dir that links to me.
  Dir       *dir;     // if i'm a dir.
  Dentry    *dn;      // if i'm linked to a dentry.
  string    *symlink; // symlink content, if its a symlink

  void get() { ref++; }
  void put() { ref--; assert(ref >= 0); }

  Inode() : ref(0), symlink(0) { }
  ~Inode() {
	if (symlink) { delete symlink; symlink = 0; }
  }
};

class Dentry : public LRUObject {
 public:
  string  name;                      // sort of lame
  Dir     *dir;
  Inode   *inode;
  int     ref;                       // 1 if there's a dir beneath me.
  
  void get() { assert(ref == 0); ref++; lru_pin(); }
  void put() { assert(ref == 1); ref--; lru_unpin(); }
  
  Dentry() : ref(0), dir(0), inode(0) { }
};


// file handle for any open file state
struct Fh {
  inodeno_t ino;
  int       mds;  // have to talk to mds we opened with (for now)
  //...
  int       lockstate;  // async read, write, locked, etc.
};


// func pointer type for returning directory contents.
// this basically matches the FUSE fill func, sans FUSE's typedefs.
typedef int (*dirfillerfunc_t) (void *handle, const char *name, int type, inodeno_t ino);



// ========================================================
// client interface

class Client : public Dispatcher {
 protected:
  MDCluster *mdcluster;
  Messenger *messenger;  
  int whoami;
  bool all_files_closed;
  
  // cache
  map<inodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // file handles
  map<fileh_t, Fh*>         fh_map;


  // global semaphore/mutex protecting cache+fh structures
  // ??

  // -- metadata cache stuff

  // decrease inode ref.  delete if dangling.
  void put_inode(Inode *in) {
	in->put();
	if (in->ref == 0) {
	  inode_map.erase(in->inode.ino);
	  delete in;
	}
  }
  // open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
  Dir *open_dir(Inode *in) {
	if (!in->dir) {
	  if (in->dn) in->dn->get();  	// pin dentry
	  in->dir = new Dir(in);
	}
	return in->dir;
  }

  int get_cache_size() { return lru.lru_get_size(); }
  void set_cache_size(int m) { lru.lru_set_max(m); }

  Dentry* link(Dir *dir, string& name, Inode *in) {
	Dentry *dn = new Dentry;
	dn->name = name;

	// link to dir
	dn->dir = dir;
	dir->dentries[name] = dn;

	// link to inode
	dn->inode = in;
	in->dn = dn;
	in->get();

	lru.lru_insert_mid(dn);    // mid or top?
	return dn;
  }
  
  void unlink(Dentry *dn) {
	Inode *in = dn->inode;

	// unlink from inode
	dn->inode = 0;
	in->dn = 0;
	in->put();

	// unlink from dir
	dn->dir->dentries.erase(dn->name);
	if (dn->dir->dentries.empty()) {   // close dir
	  if (dn->dir->parent_inode->dn)
		dn->dir->parent_inode->dn->put();
	  dn->dir->parent_inode->dir = 0;
	  put_inode(dn->dir->parent_inode);
	  delete dn->dir;
	}
	dn->dir = 0;

	// delete den
	lru.lru_remove(dn);
	delete dn;
  }

  // move dentry to top of lru
  void touch_dn(Dentry *dn) { lru.lru_touch(dn); }  

  // trim cache.
  void trim_cache() {
	while (lru.lru_get_size() > lru.lru_get_max()) {
	  Dentry *dn = (Dentry*)lru.lru_expire();
	  if (!dn) break;  // done
	  
	  unlink(dn);
	}
  }
  
  // find dentry based on filepath
  Dentry *lookup(filepath* path) {
    Inode *cur = root;
    Dentry *dn;
    for (int i=0; i<path->depth(); i++) {
      Dir *dir = open_dir(cur);
      if (dir->dentries.count(path[i]))
	dn = dir->dentries[path[i]];
      else
	return NULL;
      cur = dn->inode;
    }
    return dn;
  }
		
 public:
  Client(MDCluster *mdc, int id, Messenger *m);
  ~Client();

  void init();
  void shutdown();

  // messaging
  void dispatch(Message *m) {
	cout << "dispatch not implemented" << endl;
	assert(0);
  }

  // metadata cache
  Inode* insert_inode_info(Dir *dir, c_inode_info *in_info);
  void insert_trace(vector<c_inode_info*> trace);

  // ----------------------
  // fs ops.
  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statfs *stbuf);

  // namespace ops
  int getdir(const char *path, map<string,inode_t*>& contents);
  int link(const char *existing, const char *newname);
  int unlink(const char *path);
  int rename(const char *from, const char *to);

  // dirs
  int mkdir(const char *path, mode_t mode);
  int rmdir(const char *path);

  // symlinks
  int readlink(const char *path, char *buf, size_t size);
  int symlink(const char *existing, const char *newname);

  // inode stuff
  int lstat(const char *path, struct stat *stbuf);
  int chmod(const char *path, mode_t mode);
  int chown(const char *path, uid_t uid, gid_t gid);
  int utime(const char *path, struct utimbuf *buf);
  
  // file ops
  int mknod(const char *path, mode_t mode);
  int open(const char *path, int mode);
  int read(fileh_t fh, char *buf, size_t size, off_t offset);
  int write(fileh_t fh, const char *buf, size_t size, off_t offset);
  int truncate(fileh_t fh, off_t size);
  int fsync(fileh_t fh);

};

#endif
