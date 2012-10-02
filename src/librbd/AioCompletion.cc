// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"

#include "librbd/AioRequest.h"
#include "librbd/internal.h"

#include "librbd/AioCompletion.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

  void AioCompletion::complete_request(CephContext *cct, ssize_t r)
  {
    ldout(cct, 20) << "AioCompletion::complete_request() this="
		   << (void *)this << " complete_cb=" << (void *)complete_cb << dendl;
    lock.Lock();
    if (rval >= 0) {
      if (r < 0 && r != -EEXIST)
	rval = r;
      else if (r > 0)
	rval += r;
    }
    assert(pending_count);
    int count = --pending_count;
    if (!count) {
      complete();
    }
    put_unlock();
  }

  void C_AioRead::finish(int r)
  {
    ldout(m_cct, 10) << "C_AioRead::finish() " << this << dendl;
    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(m_cct, 10) << "ofs=" << m_req->offset()
		       << " len=" << m_req->length() << dendl;
      r = handle_sparse_read(m_cct, m_req->data(), m_req->offset(),
			     m_req->ext_map(), 0, m_req->length(),
			     m_out_buf);
    }
    m_completion->complete_request(m_cct, r);
  }

  void C_CacheRead::finish(int r)
  {
    m_completion->complete(r);
    delete m_req;
  }
}
