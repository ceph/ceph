// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_PAYLOAD_H
#define CEPH_JOURNAL_PAYLOAD_H

#include "include/int_types.h"
#include "include/buffer.h"
#include <boost/intrusive_ptr.hpp>

namespace journal {

class PayloadImpl;

class Payload {
public:
  typedef boost::intrusive_ptr<PayloadImpl> PayloadImplPtr;

  Payload() {}
  Payload(const PayloadImplPtr &payload) : m_payload_impl(payload) {}

  inline bool is_valid() const {
    return m_payload_impl;
  }

  const bufferlist &get_data() const;

private:
  friend class Journaler;

  inline PayloadImplPtr get_payload_impl() const {
    return m_payload_impl;
  }

  PayloadImplPtr m_payload_impl;
};

void intrusive_ptr_add_ref(PayloadImpl *p);
void intrusive_ptr_release(PayloadImpl *p);

} // namespace journal

#endif // CEPH_JOURNAL_PAYLOAD_H
