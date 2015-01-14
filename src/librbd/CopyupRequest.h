// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_COPYUPREQUEST_H
#define CEPH_LIBRBD_COPYUPREQUEST_H

#include "include/int_types.h"

#include "common/Mutex.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"

namespace librbd {

  struct AioCompletion;

  class CopyupRequest {
  public:
    CopyupRequest();
    CopyupRequest(ImageCtx *ictx, const std::string &oid, uint64_t objectno,
                  bool need_copyup);
    ~CopyupRequest();

    void set_ready();
    bool is_ready();
    bool is_need_send_copyup();
    ceph::bufferlist& get_copyup_data();
    Mutex& get_lock();
    void append_request(AioRequest *req);
    void complete_all(int r);
    void send_copyup(int r);
    void read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents);
    AioCompletion *get_parent_completion() { return m_parent_completion; }
    librados::AioCompletion *get_copyup_completion() { return m_copyup_completion; }
    ImageCtx *m_ictx;

  private:
    std::string m_oid;
    uint64_t m_object_no;
    Mutex m_lock;
    bool m_ready;
    bool m_need_copyup;
    AioCompletion *m_parent_completion;
    librados::AioCompletion *m_copyup_completion;
    ceph::bufferlist m_copyup_data;
    vector<AioRequest *> m_pending_requests;
  };

  void rbd_read_from_parent_cb(completion_t cb, void *arg);
  void rbd_copyup_cb(completion_t aio_completion_impl, void *arg);
}

#endif
