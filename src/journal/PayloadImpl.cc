// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/PayloadImpl.h"

namespace journal {

PayloadImpl::PayloadImpl(const bufferlist &data,
                         const ObjectSetPosition &object_set_position)
  : m_data(data), m_object_set_position(object_set_position) {
}

const bufferlist &PayloadImpl::get_data() const {
  return m_data;
}

const PayloadImpl::ObjectSetPosition &
PayloadImpl::get_object_set_position() const {
  return m_object_set_position;
}

} // namespace journal
