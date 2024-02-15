// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "osd/osd_types.h"

#include "crimson/os/futurized_collection.h"

namespace crimson::os {

class Object;
/**
 * a collection also orders transactions
 *
 * Any transactions queued under a given collection will be applied in
 * sequence.  Transactions queued under different collections may run
 * in parallel.
 *
 * ObjectStore users may get collection handles with open_collection() (or,
 * for bootstrapping a new collection, create_new_collection()).
 */
struct Collection final : public FuturizedCollection {
  using ObjectRef = boost::intrusive_ptr<Object>;
  int bits = 0;
  // always use bufferlist object for testing
  bool use_page_set = false;
  std::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
  std::map<ghobject_t, ObjectRef> object_map;        ///< for iteration
  std::map<std::string,bufferptr> xattr;
  bool exists = true;

  pool_opts_t pool_opts;

  Collection(const coll_t& c);
  ~Collection() final;

  ObjectRef create_object() const;
  ObjectRef get_object(ghobject_t oid);
  ObjectRef get_or_create_object(ghobject_t oid);
  uint64_t used_bytes() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
};

}
