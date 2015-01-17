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
    CopyupRequest(ImageCtx *ictx, const std::string &oid, uint64_t objectno,
                  bool send_copyup);
    ~CopyupRequest();

    ceph::bufferlist& get_copyup_data();
    void append_request(AioRequest *req);
    void read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents);
    void queue_read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents);

  private:
    class C_ReadFromParent : public Context {
    public:
      C_ReadFromParent(CopyupRequest *c, vector<pair<uint64_t,uint64_t> > i)
        : m_req(c), m_image_extents(i) {}

      virtual void finish(int r) {
        m_req->read_from_parent(m_image_extents);
      }

    private:
      CopyupRequest *m_req;
      vector<pair<uint64_t,uint64_t> > m_image_extents;
    };

    ImageCtx *m_ictx;
    std::string m_oid;
    uint64_t m_object_no;
    bool m_send_copyup;
    ceph::bufferlist m_copyup_data;
    vector<AioRequest *> m_pending_requests;

    void complete_all(int r);
    void send_copyup(int r);
    static void read_from_parent_cb(completion_t cb, void *arg);

  };
}

#endif
