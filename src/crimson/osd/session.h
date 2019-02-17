// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
#include "crimson/osd/op_request.h"

class Session : public boost::intrusive_ref_counter<Session,
                    boost::thread_unsafe_counter> {
public:
  boost::intrusive::list<OpRequest> waiting_on_map;


};
using SessionRef = boost::intrusive_ptr<Session>;
