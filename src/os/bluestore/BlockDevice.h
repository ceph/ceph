// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_NEWSTORE_BLOCKDEVICE
#define CEPH_OS_NEWSTORE_BLOCKDEVICE

#include "os/fs/FS.h"
#include "include/interval_set.h"

/// track in-flight io
struct IOContext {
  void *priv;

  Mutex lock;
  Cond cond;
  interval_set<uint64_t> blocks;  ///< blocks with aio in flight

  list<FS::aio_t> pending_aios;    ///< not yet submitted
  list<FS::aio_t> running_aios;    ///< submitting or submitted
  int num_pending;
  int num_running;
  int num_reading;

  bufferlist pending_bl;  // just a pile of refs
  bufferlist running_bl;  // just a pile of refs

  IOContext(void *p)
    : priv(p),
      lock("IOContext::lock"),
      num_pending(0),
      num_running(0),
      num_reading(0) {}

  bool has_aios() {
    Mutex::Locker l(lock);
    return num_pending + num_running;
  }

  void aio_wait();
  void _aio_wait();
};

class BlockDevice {
public:
  typedef void (*aio_callback_t)(void *handle, void *aio);

private:
  int fd;
  uint64_t size;
  uint64_t block_size;
  string path;
  FS *fs;
  bool aio, dio;
  bufferptr zeros;

  interval_set<uint64_t> debug_inflight;

  FS::aio_queue_t aio_queue;
  aio_callback_t aio_callback;
  void *aio_callback_priv;
  bool aio_stop;

  struct AioCompletionThread : public Thread {
    BlockDevice *bdev;
    AioCompletionThread(BlockDevice *b) : bdev(b) {}
    void *entry() {
      bdev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  void _aio_thread();
  int _aio_start();
  void _aio_stop();

  void _aio_prepare(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_finish(IOContext *ioc, uint64_t offset, uint64_t length);

public:
  BlockDevice(aio_callback_t cb, void *cbpriv);

  void aio_submit(IOContext *ioc);

  uint64_t get_size() const {
    return size;
  }
  uint64_t get_block_size() const {
    return block_size;
  }

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc);

  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc);
  int aio_zero(uint64_t off, uint64_t len,
	       IOContext *ioc);
  int flush();

  int open(string path);
  void close();
};

#endif
