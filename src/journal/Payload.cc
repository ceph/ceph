// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Payload.h"
#include "journal/PayloadImpl.h"

namespace journal {

const bufferlist &Payload::get_data() const {
  return m_payload_impl->get_data();
}

void intrusive_ptr_add_ref(PayloadImpl *p) {
  p->get();
}

void intrusive_ptr_release(PayloadImpl *p) {
  p->put();
}

} // namespace journal
