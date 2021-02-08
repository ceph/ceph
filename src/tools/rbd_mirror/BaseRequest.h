// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_BASE_REQUEST_H
#define CEPH_RBD_MIRROR_BASE_REQUEST_H

#include "include/Context.h"

namespace rbd {
namespace mirror {

class BaseRequest  {
public:
  BaseRequest(Context *on_finish) : m_on_finish(on_finish) {
  }
  virtual ~BaseRequest() {}

  virtual void send() = 0;

protected:
  virtual void finish(int r) {
    m_on_finish->complete(r);
    delete this;
  }

private:
  Context *m_on_finish;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_BASE_REQUEST_H
