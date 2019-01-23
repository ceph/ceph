// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"

template<typename T> using Ref = boost::intrusive_ptr<T>;

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>
{
  using ec_profile_t = std::map<std::string,std::string>;
public:
  PG(pg_pool_t&& pool, std::string&& name, ec_profile_t&& ec_profile);
};
