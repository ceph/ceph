// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <unistd.h>
#include "dumb_backend.h"

string DumbBackend::get_full_path(const string &oid)
{
	return path + "/" + oid;
}

void DumbBackend::_write(
  const string &oid,
  uint64_t offset,
  const bufferlist &bl,
  Context *on_applied,
  Context *on_commit)
{
  string full_path(get_full_path(oid));
  int fd = ::open(
    full_path.c_str(), O_CREAT|O_WRONLY, 0777);
  if (fd < 0) {
    std::cout << full_path << ": errno is " << errno << std::endl;
    assert(0);
  }

  int r =  ::lseek(fd, offset, SEEK_SET);
  if (r < 0) {
    r = errno;
    std::cout << "lseek failed, errno is: " << r << std::endl;
    ::close(fd);
    return;
  }
  bl.write_fd(fd);
  on_applied->complete(0);
  if (do_fsync)
    ::fsync(fd);
  if (do_sync_file_range)
    ::sync_file_range(fd, offset, bl.length(),
		      SYNC_FILE_RANGE_WAIT_AFTER);
  if (do_fadvise) {
    int fa_r = ::posix_fadvise(fd, offset, bl.length(), POSIX_FADV_DONTNEED);
    if (fa_r) {
        std::cout << "posix_fadvise failed, errno is: " << fa_r << std::endl;
    }
  }
  ::close(fd);
  {
    Mutex::Locker l(pending_commit_mutex);
    pending_commits.insert(on_commit);
  }
  sem.Put();
}

void DumbBackend::read(
  const string &oid,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  Context *on_complete)
{
  string full_path(get_full_path(oid));
  int fd = ::open(
    full_path.c_str(), 0, O_RDONLY);
  if (fd < 0) return;

  int r = ::lseek(fd, offset, SEEK_SET);
  if (r < 0) {
    r = errno;
    std::cout << "lseek failed, errno is: " << r << std::endl;
    ::close(fd);
    return;
  }

  bl->read_fd(fd, length);
  ::close(fd);
  on_complete->complete(0);
}

void DumbBackend::sync_loop()
{
  while (1) {
    sleep(sync_interval);
    {
      Mutex::Locker l(sync_loop_mutex);
      if (sync_loop_stop != 0) {
	sync_loop_stop = 2;
	sync_loop_cond.Signal();
	break;
      }
    }
    tp.pause();
#ifdef HAVE_SYS_SYNCFS
    ::syncfs(sync_fd);
#else
    ::sync();
#endif
    {
      Mutex::Locker l(pending_commit_mutex);
      for (set<Context*>::iterator i = pending_commits.begin();
	   i != pending_commits.end();
	   pending_commits.erase(i++)) {
	(*i)->complete(0);
      }
    }
    tp.unpause();
  }
}
