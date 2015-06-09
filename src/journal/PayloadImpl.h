// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_PAYLOAD_IMPL_H
#define CEPH_JOURNAL_PAYLOAD_IMPL_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/RefCountedObj.h"
#include "cls/journal/cls_journal_types.h"
#include <boost/noncopyable.hpp>
#include <boost/intrusive_ptr.hpp>
#include "include/assert.h"

namespace journal {

class PayloadImpl;
typedef boost::intrusive_ptr<PayloadImpl> PayloadImplPtr;

class PayloadImpl : public RefCountedObject, boost::noncopyable {
public:
  typedef cls::journal::ObjectSetPosition ObjectSetPosition;

  PayloadImpl(const bufferlist &data,
              const ObjectSetPosition &object_set_position);

  const bufferlist &get_data() const;
  const ObjectSetPosition &get_object_set_position() const;

private:
  bufferlist m_data;
  ObjectSetPosition m_object_set_position;
};

} // namespace journal

#endif // CEPH_JOURNAL_PAYLOAD_IMPL_H
