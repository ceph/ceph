#ifndef __CLIENT_H
#define __CLIENT_H

#include "msg/Message.h"
#include "msgthread.h"

#include "include/types.h"

#include <set>
#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;



// ============================================
// types for my local metadata cache
/* basic structure:
   
 - dentries live in an LRU loop.  they get expired based on last access.
 - inode has ref count for each fh, dir, or dentry that points to it.
 - when inode ref goes to 0, it's expired.
 - when dir is empty, it's removed (and it's inode ref--)
 
*/

class Dentry;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  hash_map< string, Dentry* > dentries;

  Dir(Inode* in) { inode = in; }
};

class Inode {
 public:
  inodeno_t inode;
  time_t    last_updated;

  int       ref;      // ref count. 1 for each dentry, fh or dir that links to me
  Dir       *dir;     // if i'm a dir.

  void get() { ref++; }
  void put() { ref--; assert(ref >= 0); }

  Inode() : ref(0) { }
};

class Dentry : public LRUObject {
 public:
  string  name;                      // sort of lame
  Dentry  *dn;
  Inode   *inode;
  int     ref;                       // 1 if there's a dir beneath me.
  
  void get() { assert(ref == 0); ref++; lru_pin(); }
  void put() { assert(ref == 1); ref--; lru_unpin(); }
  
  Dentry() : ref(0) { }
};

// file handle for any open file state
struct Fh {
  inodeno_t ino;
  int       mds;  // have to talk to mds we opened with (for now)
  //...
  int       lockstate;  // async read, write, locked, etc.
};



// ========================================================
// client interface

class Client {
 protected:
  // cache
  map<inodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // file handles
  map<fh_t, Fh*>         fh_map;


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

  // move dentry to top of lru
  void touch_dn(Dentry *dn) { lru.lru_touch(dn); }  

  // trim cache.
  void trim_cache() {
	while (lru.lru_get_size() > lru.lru_get_max()) {
	  Dentry *dn = (Dentry*)lru.lru_expire();
	  if (!dn) break;  // done
	  
	  // unlink from inode
	  dn->inode->dn = 0;
	  put_inode(dn->inode);

	  // unlink from dir
	  dn->dir->dentries.erase(dn->name);
	  if (dn->dir->dentries.size() == 0) {
		if (dn->dir->parent_inode->dn) {
		  dn->dir->parent_inode->dn->put();
		}
		put_inode(dn->dir->inode);
		delete dn->dir;
	  }

	  // hose
	  delete dn;
	}
  }
  
 public:
  Client();
  ~Client();

  // ----------------------
  // fs ops.
  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statfs *stbuf);

  // namespace ops
  //?int getdir(const char *path, fuse_dirh_t h, fuse_dirfil_t filler);
  int link(const char *existing, const char *new);
  int unlink(const char *path);
  int rename(const char *from, const char *to);

  // dirs
  int mkdir(const char *path, mode_t mode);
  int rmdir(const char *path);

  // symlinks
  int readlink(const char *path, char *buf, size_t size);
  int symlink(const char *existing, const char *new);

  // inode stuff
  int lstat(const char *path, struct stat *stbuf);
  int chmod(const char *path, mode_t mode);
  int chown(const char *path, uid_t uid, gid_t gid);
  int utime(const char *path, struct utimbuf *buf);
  
  // file ops
  int mknod(const char *path, mode_t mode);
  int open(const char *path, int mode);
  int read(fh_t fh, char *buf, size_t size, off_t offset);
  int write(fh_t fh, const char *buf, size_t size, off_t offset);
  int truncate(fh_t fh, off_t size);
  int fsync(fh_t fh);

};

#endif
