// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_BASE_REQUEST_H
#define CEPH_RBD_MIRROR_BASE_REQUEST_H

#include "common/RefCountedObj.h"
#include "include/Context.h"

namespace rbd {
namespace mirror {

class BaseRequest : public RefCountedObject {
public:
  BaseRequest(const std::string& name, CephContext *cct, Context *on_finish)
    : RefCountedObject(cct, 1), m_name(name), m_cct(cct),
      m_on_finish(on_finish) {
  }

  virtual void send() = 0;
  virtual void cancel() {}

protected:
  virtual void finish(int r) {
    if (m_cct) {
      lsubdout(m_cct, rbd_mirror, 20) << m_name << "::finish: r=" << r << dendl;
    }
    if (m_on_finish) {
      m_on_finish->complete(r);
    }
    put();
  }

private:
  const std::string m_name;
  CephContext *m_cct;
  Context *m_on_finish;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_BASE_REQUEST_H
