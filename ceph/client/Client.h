#ifndef __CLIENT_H
#define __CLIENT_H

#include "msg/Message.h"
#include "msgthread.h"

#include "include/types.h"

#include <set>
#include <map>
#include <hash_map>
using namespace std;

// types for my local metadata cache
struct Inode {
};
struct Dir {
};
struct Dentry {
};

// file handle for any open file state
struct Fh {
  inodeno_t ino;
  //...
};

class Client {
 protected:
  // cache
  map<inodeno_t, Inode*> inode_map;
  Inode*                 root;

  // file handles
  map<fh_t, Fh*>         fh_map;

  // global semaphore/mutex protecting cache+fh structures
  // ??

  
 public:
  Client();
  ~Client();

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
  int getattr(const char *path, struct stat *stbuf);
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
