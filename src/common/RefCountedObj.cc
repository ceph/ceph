// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//
#include "include/ceph_assert.h"

#include "common/RefCountedObj.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/valgrind.h"

namespace TOPNSPC::common {
RefCountedObject::~RefCountedObject()
{
  ceph_assert(nref == 0);
}

void RefCountedObject::put() const {
  CephContext *local_cct = cct;
  auto v = --nref;
  if (local_cct) {
    lsubdout(local_cct, refs, 1) << "RefCountedObject::put " << this << " "
		   << (v + 1) << " -> " << v
		   << dendl;
  }
  if (v == 0) {
    ANNOTATE_HAPPENS_AFTER(&nref);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&nref);
    delete this;
  } else {
    ANNOTATE_HAPPENS_BEFORE(&nref);
  }
}

void RefCountedObject::_get() const {
  auto v = ++nref;
  ceph_assert(v > 1); /* it should never happen that _get() sees nref == 0 */
  if (cct) {
    lsubdout(cct, refs, 1) << "RefCountedObject::get " << this << " "
	     << (v - 1) << " -> " << v << dendl;
  }
}

}
