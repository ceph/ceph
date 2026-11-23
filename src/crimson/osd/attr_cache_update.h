// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <span>
#include <string>

#include "common/hobject.h"
#include "include/buffer.h"
#include "os/Transaction.h"

namespace crimson::osd {

/// the xattrs of one object, as in ObjectContext::attr_cache
using attr_map_t = std::map<std::string, ceph::buffer::list, std::less<>>;

/// one object whose xattrs update_attr_cache() keeps up to date
struct tracked_attrs_t {
  const hobject_t* oid = nullptr;
  attr_map_t* attrs = nullptr;
};

/**
 * update_attr_cache
 *
 * Apply the xattr changes of txn to the xattr maps in objs.
 *
 * The ops of a request write only to txn. As on the classic OSD, the
 * attr_cache of an obc changes when the request is submitted, so all the
 * xattr reads of a request see the xattrs as they were before the
 * request. On EC pools, ECTransaction does this. On replicated pools,
 * ReplicatedBackend::submit_transaction() calls this function: it replays
 * the ops of txn in order, as the store will apply them, so each map
 * holds the same xattrs as the store after the transaction.
 *
 * An op on an object that is not in objs does not change any map.
 *
 * Aborts on an op that it does not know, and on a CLONE to an object in
 * objs from an object that is not in objs: in both cases the map could no
 * longer match the store, and a wrong attr_cache fails silently later.
 */
void update_attr_cache(
  ceph::os::Transaction& txn,
  std::span<const tracked_attrs_t> objs);

} // namespace crimson::osd
