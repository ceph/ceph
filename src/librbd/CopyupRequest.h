// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_COPYUPREQUEST_H
#define CEPH_LIBRBD_COPYUPREQUEST_H

#include "librbd/AsyncOperation.h"
#include "include/int_types.h"

#include "common/Mutex.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"

namespace librbd {

  struct AioCompletion;

  class CopyupRequest {
  public:
    CopyupRequest(ImageCtx *ictx, const std::string &oid, uint64_t objectno,
                  vector<pair<uint64_t,uint64_t> >& image_extents);
    ~CopyupRequest();

    ceph::bufferlist& get_copyup_data();
    void append_request(AioRequest *req);

    void send();
    void queue_send();

  private:
    /**
     * Copyup requests go through the following state machine to read from the
     * parent image, update the object map, and copyup the object:
     *
     * <start>
     *    |
     *    v
     * STATE_READ_FROM_PARENT ---> STATE_OBJECT_MAP
     *    .                           |
     *    . . . . . . . . . . . . .   |
     *                            .   |
     *                            v   v
     *                           <finish>
     * The _OBJECT_MAP state is skipped if the object map isn't enabled.
     */
    enum State {
      STATE_READ_FROM_PARENT,
      STATE_OBJECT_MAP
    };

    ImageCtx *m_ictx;
    std::string m_oid;
    uint64_t m_object_no;
    vector<pair<uint64_t,uint64_t> > m_image_extents;
    State m_state;
    ceph::bufferlist m_copyup_data;
    vector<AioRequest *> m_pending_requests;

    AsyncOperation m_async_op;

    bool complete_requests(int r);

    void complete(int r);
    bool should_complete(int r);

    void remove_from_list();

    bool send_object_map(); 
    void send_copyup();

    Context *create_callback_context();
  };
}

#endif
