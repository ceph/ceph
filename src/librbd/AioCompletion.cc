// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"

#include "librbd/internal.h"

#include "librbd/AioCompletion.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

  void AioBlockCompletion::finish(int r)
  {
    ldout(cct, 10) << "AioBlockCompletion::finish()" << dendl;
    if ((r >= 0 || r == -ENOENT) && buf) { // this was a sparse_read operation
      ldout(cct, 10) << "ofs=" << ofs << " len=" << len << dendl;
      r = handle_sparse_read(cct, data_bl, ofs, m, 0, len, simple_read_cb, buf);
    }
    completion->complete_block(this, r);
  }

  void AioCompletion::complete_block(AioBlockCompletion *block_completion, ssize_t r)
  {
    CephContext *cct = block_completion->cct;
    ldout(cct, 20) << "AioCompletion::complete_block() this=" 
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
}
