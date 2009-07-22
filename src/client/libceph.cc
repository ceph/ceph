#include "libceph.h"

#include <string.h>
#include <fcntl.h>
#include <iostream>

#include "common/Mutex.h"
#include "messages/MMonMap.h"
#include "common/common_init.h"
#include "Client.h"
#include "msg/SimpleMessenger.h"

/* ************* ************* ************* *************
 * C interface
 */

static Mutex ceph_client_mutex("ceph_client");
static int client_initialized = 0;
static Client *client = NULL;
static MonClient *monclient = NULL;
static SimpleMessenger *rank = NULL;

extern "C" int ceph_initialize(int argc, const char **argv)
{  ceph_client_mutex.Lock();
  if (!client_initialized) {
    //create everything to start a client
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    common_init(args, "libceph", false);
    if (g_conf.clock_tare) g_clock.tare();
    //monmap
    monclient = new MonClient();
    if (monclient->build_initial_monmap() < 0) {
      delete monclient;
      return -1; //error!
    }
    //network connection
    rank = new SimpleMessenger();
    rank->bind();

    //at last the client
    client = new Client(rank->register_entity(entity_name_t::CLIENT()), monclient);

    rank->start();
    rank->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::lossy_fast_fail());
    rank->set_policy(entity_name_t::TYPE_MDS, SimpleMessenger::Policy::lossless());
    rank->set_policy(entity_name_t::TYPE_OSD, SimpleMessenger::Policy::lossless());

    client->init();

    ++client_initialized;
  }
  ceph_client_mutex.Unlock();
  return 0;
}

extern "C" void ceph_deinitialize()
{
  ceph_client_mutex.Lock();
  client->unmount();
  client->shutdown();
  delete client;
  rank->wait();
  delete rank;
  delete monclient;
  --client_initialized;
  ceph_client_mutex.Unlock();
}

extern "C" int ceph_mount()
{
  return client->mount();
}

extern "C" int ceph_umount()
{
  return client->unmount();
}

extern "C" int ceph_statfs(const char *path, struct statvfs *stbuf)
{
  return client->statfs(path, stbuf);
}
  
extern "C" int ceph_chdir (const char *s)
{
  return client->chdir(s);
}

extern "C" int ceph_opendir(const char *name, DIR **dirpp)
{
  return client->opendir(name, dirpp);
}

extern "C" int ceph_closedir(DIR *dirp)
{
  return client->closedir(dirp);
}

extern "C" int ceph_readdir_r(DIR *dirp, struct dirent *de)
{
  return client->readdir_r(dirp, de);
}

extern "C" int ceph_readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask)
{
  return client->readdirplus_r(dirp, de, st, stmask);
}

extern "C" void ceph_rewinddir(DIR *dirp)
{
  client->rewinddir(dirp);
}

extern "C" loff_t ceph_telldir(DIR *dirp)
{
  return client->telldir(dirp);
}

extern "C" void ceph_seekdir(DIR *dirp, loff_t offset)
{
  client->seekdir(dirp, offset);
}

extern "C" int ceph_link (const char *existing, const char *newname)
{
  return client->link(existing, newname);
}

extern "C" int ceph_unlink (const char *path)
{
  return client->unlink(path);
}

extern "C" int ceph_rename(const char *from, const char *to)
{
  return client->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(const char *path, mode_t mode)
{
  return client->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(const char *path, mode_t mode)
{
  return client->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(const char *path)
{
  return client->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(const char *path, char *buf, loff_t size)
{
  return client->readlink(path, buf, size);
}

extern "C" int ceph_symlink(const char *existing, const char *newname)
{
  return client->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_lstat(const char *path, struct stat *stbuf, struct frag_info_t *dirstat)
{
  return client->lstat(path, stbuf, dirstat);
}

extern "C" int ceph_setattr(const char *relpath, struct stat *attr, int mask)
{
  return client->setattr(relpath, attr, mask);
}

extern "C" int ceph_chmod(const char *path, mode_t mode)
{
  return client->chmod(path, mode);
}
extern "C" int ceph_chown(const char *path, uid_t uid, gid_t gid)
{
  return client->chown(path, uid, gid);
}

extern "C" int ceph_utime(const char *path, struct utimbuf *buf)
{
  return client->utime(path, buf);
}

extern "C" int ceph_truncate(const char *path, loff_t size)
{
  return client->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(const char *path, mode_t mode, dev_t rdev)
{
  return client->mknod(path, mode, rdev);
}

extern "C" int ceph_open(const char *path, int flags, mode_t mode)
{
  return client->open(path, flags, mode);
}

extern "C" int ceph_close(int fd)
{
  return client->close(fd);
}

extern "C" loff_t ceph_lseek(int fd, loff_t offset, int whence)
{
  return client->lseek(fd, offset, whence);
}

extern "C" int ceph_read(int fd, char *buf, loff_t size, loff_t offset)
{
  return client->read(fd, buf, size, offset);
}

extern "C" int ceph_write(int fd, const char *buf, loff_t size, loff_t offset)
{
  return client->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(int fd, loff_t size)
{
  return client->ftruncate(fd, size);
}

extern "C" int ceph_fsync(int fd, bool syncdataonly)
{
  return client->fsync(fd, syncdataonly);
}

extern "C" int ceph_fstat(int fd, struct stat *stbuf)
{
  return client->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs()
{
  return client->sync_fs();
}

extern "C" int ceph_get_stripe_unit(int fh)
{
  return client->get_stripe_unit(fh);
}

int ceph_getdir(const char *relpath, std::list<std::string>& names)
{
  return client->getdir(relpath, names);
}
