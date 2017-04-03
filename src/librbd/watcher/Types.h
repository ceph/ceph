// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_TYPES_H
#define CEPH_LIBRBD_WATCHER_TYPES_H

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/Context.h"

namespace ceph {
class Formatter;
}

namespace librbd {

class Watcher;

namespace watcher {

struct C_NotifyAck : public Context {
  Watcher *watcher;
  CephContext *cct;
  uint64_t notify_id;
  uint64_t handle;
  bufferlist out;

  C_NotifyAck(Watcher *watcher, uint64_t notify_id, uint64_t handle);
  void finish(int r) override;
};

template <typename Watcher>
struct HandlePayloadVisitor : public boost::static_visitor<void> {
  Watcher *watcher;
  uint64_t notify_id;
  uint64_t handle;

  HandlePayloadVisitor(Watcher *watcher_, uint64_t notify_id_,
      uint64_t handle_)
    : watcher(watcher_), notify_id(notify_id_), handle(handle_)
  {
  }

  template <typename P>
  inline void operator()(const P &payload) const {
    C_NotifyAck *ctx = new C_NotifyAck(watcher, notify_id, handle);
    if (watcher->handle_payload(payload, ctx)) {
      ctx->complete(0);
    }
  }
};

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename P>
  inline void operator()(const P &payload) const {
    ::encode(static_cast<uint32_t>(P::NOTIFY_OP), m_bl);
    payload.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodePayloadVisitor : public boost::static_visitor<void> {
public:
  DecodePayloadVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {}

  template <typename P>
  inline void operator()(P &payload) const {
    payload.decode(m_version, m_iter);
  }

private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

template <typename ImageCtxT>
struct Traits {
  typedef librbd::Watcher Watcher;
};

} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_TYPES_H
