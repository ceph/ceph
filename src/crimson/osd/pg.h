// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>
#include "osd/osd_types.h"
#include "crimson/osd/op_request.h"
#include "crimson/osd/pg_backend.h"

template<typename T> using Ref = boost::intrusive_ptr<T>;
class PG;

using PGRef = boost::intrusive_ptr<PG>;

class PG : public boost::intrusive_ref_counter<PG,
                  boost::thread_unsafe_counter>
{
  using ec_profile_t = std::map<std::string,std::string>;
public:
  seastar::future<> do_request(OpRef op);
  PG(pg_pool_t&& pool, std::string&& name, ec_profile_t&& ec_profile);
  ~PG(){}

  seastar::shared_mutex sm;
  boost::intrusive_ptr<PGBackend> pgbackend;

  bool is_deleting() const {
    return deleting;
  }

protected:
  uint64_t    state;   // PG_STATE_*
  bool deleting;  // true while in removing or OSD is shutting down
  bool can_discard_request(OpRef op);
  seastar::future<bool>handle_backoff_feature(OpRef op);
  seastar::future<bool>handle_not_peered(OpRef op);

  list<OpRef>            waiting_for_active;
  bool state_test(uint64_t m) const { return (state & m) != 0; }
  bool is_active() const { return state_test(PG_STATE_ACTIVE); }
};

