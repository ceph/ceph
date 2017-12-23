// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_UTILS_H
#define CEPH_LIBRBD_WATCHER_UTILS_H

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/Context.h"
#include "librbd/Watcher.h"

namespace ceph { class Formatter; }

namespace librbd {
namespace watcher {
namespace util {

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
    typename Watcher::C_NotifyAck *ctx =
      new typename Watcher::C_NotifyAck(watcher, notify_id, handle);
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
    using ceph::encode;
    encode(static_cast<uint32_t>(P::NOTIFY_OP), m_bl);
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

} // namespace util
} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_UTILS_H
