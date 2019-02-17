// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include "crimson/osd/op_request.h"

/**
 * PGBackend
 *
 * PGBackend defines an interface for logic handling IO and
 * replication on RADOS objects.  The PGBackend implementation
 * is responsible for:
 *
 * 1) Handling client operations
 * 2) Handling object recovery
 * 3) Handling object access
 * 4) Handling scrub, deep-scrub, repair
 */
template<typename T> using Ref = boost::intrusive_ptr<T>;
class PGBackend :public boost::intrusive_ref_counter<PGBackend,
                	       boost::thread_unsafe_counter> {
public:
  PGBackend() {}
  ~PGBackend() {}
  seastar::future<> handle_message(OpRef op);
  seastar::future<bool> do_op(OpRef op);
  seastar::future<> do_msg_osd_op(OpRef op);
private:
  seastar::future<int> do_osd_op(Ref<MOSDOp> m);
  
};
