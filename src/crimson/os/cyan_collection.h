// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <unordered_map>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "osd/osd_types.h"

namespace ceph::os {

class Object;
/**
 * a collection also orders transactions
 *
 * Any transactions queued under a given collection will be applied in
 * sequence.  Transactions queued under different collections may run
 * in parallel.
 *
 * ObjectStore users my get collection handles with open_collection() (or,
 * for bootstrapping a new collection, create_new_collection()).
 */
struct Collection : public boost::intrusive_ref_counter<
  Collection,
  boost::thread_unsafe_counter>
{
  using ObjectRef = boost::intrusive_ptr<Object>;
  const coll_t cid;
  int bits = 0;
  // always use bufferlist object for testing
  bool use_page_set = false;
  std::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
  std::map<ghobject_t, ObjectRef> object_map;        ///< for iteration
  std::map<std::string,bufferptr> xattr;
  bool exists = true;

  Collection(const coll_t& c);
  ~Collection();

  ObjectRef create_object() const;
  ObjectRef get_object(ghobject_t oid);
  ObjectRef get_or_create_object(ghobject_t oid);
  uint64_t used_bytes() const;

  const coll_t &get_cid() const {
    return cid;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
};

}
