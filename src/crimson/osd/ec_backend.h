// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include "include/buffer_fwd.h"
#include "osd/osd_types.h"
#include "pg_backend.h"

class ECBackend : public PGBackend
{
public:
  ECBackend(shard_id_t shard,
	    CollectionRef, ceph::os::CyanStore*,
	    const ec_profile_t& ec_profile,
	    uint64_t stripe_width);
private:
  seastar::future<ceph::bufferlist> _read(const hobject_t& hoid,
					  uint64_t off,
					  uint64_t len,
					  uint32_t flags) override;
  CollectionRef coll;
  ceph::os::CyanStore* store;
};
