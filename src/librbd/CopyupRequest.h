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

    void append_request(AioObjectRequest *req);

    void send();

    void complete(int r);

  private:
    /**
     * Copyup requests go through the following state machine to read from the
     * parent image, update the object map, and copyup the object:
     *
     *
     * @verbatim
     *
     * <start>
     *    |
     *    v
     *  STATE_READ_FROM_PARENT
     *    .   .        |
     *    .   .        v
     *    .   .     STATE_OBJECT_MAP . .
     *    .   .        |               .
     *    .   .        v               .
     *    .   . . > STATE_COPYUP       .
     *    .            |               .
     *    .            v               .
     *    . . . . > <finish> < . . . . .
     *
     * @endverbatim
     *
     * The _OBJECT_MAP state is skipped if the object map isn't enabled or if
     * an object map update isn't required. The _COPYUP state is skipped if
     * no data was read from the parent *and* there are no additional ops.
     */
    enum State {
      STATE_READ_FROM_PARENT,
      STATE_OBJECT_MAP,
      STATE_COPYUP
    };

    ImageCtx *m_ictx;
    std::string m_oid;
    uint64_t m_object_no;
    vector<pair<uint64_t,uint64_t> > m_image_extents;
    State m_state;
    ceph::bufferlist m_copyup_data;
    vector<AioObjectRequest *> m_pending_requests;
    atomic_t m_pending_copyups;

    AsyncOperation m_async_op;

    std::vector<uint64_t> m_snap_ids;

    void complete_requests(int r);

    bool should_complete(int r);

    void remove_from_list();

    bool send_object_map();
    bool send_copyup();
    bool is_nop();
  };
}

#endif
