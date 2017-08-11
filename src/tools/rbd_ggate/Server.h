// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_GGATE_SERVER_H
#define CEPH_RBD_GGATE_SERVER_H

#include "include/rbd/librbd.hpp"
#include "include/xlist.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"

namespace rbd {
namespace ggate {

class Driver;
struct Request;

class Server {
public:
  Server(Driver *drv, librbd::Image& image);

  void run();

private:
  struct IOContext {
    xlist<IOContext*>::item item;
    Server *server;
    Request *req = nullptr;

    IOContext(Server *server) : item(this), server(server) {
    }
  };

  class ThreadHelper : public Thread {
  public:
    typedef void (Server::*entry_func)();

    ThreadHelper(Server *server, entry_func func)
      : server(server), func(func) {
    }

  protected:
    virtual void* entry() {
      (server->*func)();
      return nullptr;
    }

  private:
    Server *server;
    entry_func func;
  };

  friend std::ostream &operator<<(std::ostream &os, const IOContext &ctx);

  Driver *m_drv;
  librbd::Image &m_image;

  mutable Mutex m_lock;
  Cond m_cond;
  bool m_stopping = false;
  ThreadHelper m_reader_thread, m_writer_thread;
  xlist<IOContext*> m_io_pending;
  xlist<IOContext*> m_io_finished;

  static void aio_callback(librbd::completion_t cb, void *arg);

  int start();
  void stop();

  void reader_entry();
  void writer_entry();

  void io_start(IOContext *ctx);
  void io_finish(IOContext *ctx);

  IOContext *wait_io_finish();
  void wait_clean();

  void handle_aio(IOContext *ctx, int r);
};

std::ostream &operator<<(std::ostream &os, const Server::IOContext &ctx);

} // namespace ggate
} // namespace rbd

#endif // CEPH_RBD_GGATE_SERVER_H
