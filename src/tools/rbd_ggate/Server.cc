// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "Driver.h"
#include "Server.h"
#include "Request.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd::ggate::Server: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace ggate {

Server::Server(Driver *drv, librbd::Image& image)
  : m_drv(drv), m_image(image), m_lock("rbd::ggate::Server::m_lock"),
    m_reader_thread(this, &Server::reader_entry),
    m_writer_thread(this, &Server::writer_entry) {
}

void Server::run() {
  dout(10) << dendl;

  int r = start();
  assert(r == 0);

  dout(20) << "entering run loop" << dendl;

  {
    Mutex::Locker locker(m_lock);
    while (!m_stopping) {
      m_cond.WaitInterval(m_lock, utime_t(1, 0));
    }
  }

  dout(20) << "exiting run loop" << dendl;

  stop();
}

int Server::start() {
  dout(10) << dendl;

  m_reader_thread.create("rbd_reader");
  m_writer_thread.create("rbd_writer");
  return 0;
}

void Server::stop() {
  dout(10) << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_stopping);
  }

  m_reader_thread.join();
  m_writer_thread.join();

  wait_clean();
}

void Server::io_start(IOContext *ctx) {
  dout(20) << ctx << dendl;

  Mutex::Locker locker(m_lock);
  m_io_pending.push_back(&ctx->item);
}

void Server::io_finish(IOContext *ctx) {
  dout(20) << ctx << dendl;

  Mutex::Locker locker(m_lock);
  assert(ctx->item.is_on_list());

  ctx->item.remove_myself();
  m_io_finished.push_back(&ctx->item);
  m_cond.Signal();
}

Server::IOContext *Server::wait_io_finish() {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  while (m_io_finished.empty() && !m_stopping) {
    m_cond.Wait(m_lock);
  }

  if (m_io_finished.empty()) {
    return nullptr;
  }

  IOContext *ret = m_io_finished.front();
  m_io_finished.pop_front();

  return ret;
}

void Server::wait_clean() {
  dout(20) << dendl;

  assert(!m_reader_thread.is_started());

  Mutex::Locker locker(m_lock);

  while (!m_io_pending.empty()) {
    m_cond.Wait(m_lock);
  }

  while (!m_io_finished.empty()) {
    ceph::unique_ptr<IOContext> free_ctx(m_io_finished.front());
    m_io_finished.pop_front();
  }
}

void Server::aio_callback(librbd::completion_t cb, void *arg) {
  librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(cb);

  IOContext *ctx = reinterpret_cast<IOContext *>(arg);
  int r = aio_completion->get_return_value();

  ctx->server->handle_aio(ctx, r);
  aio_completion->release();
}

void Server::handle_aio(IOContext *ctx, int r) {
  dout(20) << ctx << ": r=" << r << dendl;

  if (r == -EINVAL) {
    // if shrinking an image, a pagecache writeback might reference
    // extents outside of the range of the new image extents
    dout(5) << "masking IO out-of-bounds error" << dendl;
    ctx->req->bl.clear();
    r = 0;
  }

  if (r < 0) {
    ctx->req->set_error(-r);
  } else if ((ctx->req->get_cmd() == Request::Read) &&
             r != static_cast<int>(ctx->req->get_length())) {
    int pad_byte_count = static_cast<int> (ctx->req->get_length()) - r;
    ctx->req->bl.append_zero(pad_byte_count);
    dout(20) << ctx << ": pad byte count: " << pad_byte_count << dendl;
    ctx->req->set_error(0);
  } else {
    ctx->req->set_error(0);
  }
  io_finish(ctx);
}

void Server::reader_entry() {
  dout(20) << dendl;

  while (!m_stopping) {
    ceph::unique_ptr<IOContext> ctx(new IOContext(this));

    dout(20) << "waiting for ggate request" << dendl;

    int r = m_drv->recv(&ctx->req);
    if (r < 0) {
      if (r != -ECANCELED) {
        derr << "recv: " << cpp_strerror(r) << dendl;
      }
      Mutex::Locker locker(m_lock);
      m_stopping = true;
      m_cond.Signal();
      return;
    }

    IOContext *pctx = ctx.release();

    dout(20) << pctx << ": start: " << *pctx << dendl;

    io_start(pctx);
    librbd::RBD::AioCompletion *c =
      new librbd::RBD::AioCompletion(pctx, aio_callback);
    switch (pctx->req->get_cmd())
    {
    case rbd::ggate::Request::Write:
      m_image.aio_write(pctx->req->get_offset(), pctx->req->get_length(),
                        pctx->req->bl, c);
      break;
    case rbd::ggate::Request::Read:
      m_image.aio_read(pctx->req->get_offset(), pctx->req->get_length(),
                       pctx->req->bl, c);
      break;
    case rbd::ggate::Request::Flush:
      m_image.aio_flush(c);
      break;
    case rbd::ggate::Request::Discard:
      m_image.aio_discard(pctx->req->get_offset(), pctx->req->get_length(), c);
      break;
    default:
      derr << pctx << ": invalid request command: " << pctx->req->get_cmd()
           << dendl;
      c->release();
      Mutex::Locker locker(m_lock);
      m_stopping = true;
      m_cond.Signal();
      return;
    }
  }
  dout(20) << "terminated" << dendl;
}

void Server::writer_entry() {
  dout(20) << dendl;

  while (!m_stopping) {
    dout(20) << "waiting for io request" << dendl;

    ceph::unique_ptr<IOContext> ctx(wait_io_finish());
    if (!ctx) {
      dout(20) << "no io requests, terminating" << dendl;
      return;
    }

    dout(20) << ctx.get() << ": got: " << *ctx << dendl;

    int r = m_drv->send(ctx->req);
    if (r < 0) {
      derr << ctx.get() << ": send: " << cpp_strerror(r) << dendl;
      Mutex::Locker locker(m_lock);
      m_stopping = true;
      m_cond.Signal();
      return;
    }
    dout(20) << ctx.get() << " finish" << dendl;
  }
  dout(20) << "terminated" << dendl;
}

std::ostream &operator<<(std::ostream &os, const Server::IOContext &ctx) {

  os << "[" << ctx.req->get_id();

  switch (ctx.req->get_cmd())
  {
  case rbd::ggate::Request::Write:
    os << " Write ";
    break;
  case rbd::ggate::Request::Read:
    os << " Read ";
    break;
  case rbd::ggate::Request::Flush:
    os << " Flush ";
    break;
  case rbd::ggate::Request::Discard:
    os << " Discard ";
    break;
  default:
    os << " Unknow(" << ctx.req->get_cmd() << ") ";
    break;
  }

  os << ctx.req->get_offset() << "~" << ctx.req->get_length() << " "
     << ctx.req->get_error() << "]";

  return os;
}

} // namespace ggate
} // namespace rbd

