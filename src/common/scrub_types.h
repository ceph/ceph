// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_SCRUB_TYPES_H
#define CEPH_SCRUB_TYPES_H

#include "include/rados/rados_types.hpp"
#include "common/hobject.h"
#include "osd/osd_types.h"


// wrappers around scrub types to offer the necessary bits other than
// the minimal set that the lirados requires
struct object_id_wrapper : public librados::object_id_t {
  object_id_wrapper(const hobject_t& hoid)
    : object_id_t{hoid.oid.name, hoid.nspace, hoid.get_key(), hoid.snap}
  {}
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
};

WRITE_CLASS_ENCODER(object_id_wrapper)

inline void decode(librados::object_id_t& obj, bufferlist::iterator& bp) {
  reinterpret_cast<object_id_wrapper&>(obj).decode(bp);
}

struct shard_info_wrapper : public librados::shard_info_t {
public:
  shard_info_wrapper() = default;
  shard_info_wrapper(const ScrubMap::object& object) {
    set_object(object);
  }
  void set_object(const ScrubMap::object& object);
  void set_missing() {
    errors |= err_t::SHARD_MISSING;
  }
  void set_omap_digest_mismatch() {
    errors |= err_t::OMAP_DIGEST_MISMATCH;
  }
  void set_omap_digest_mismatch_oi() {
    errors |= err_t::OMAP_DIGEST_MISMATCH_OI;
  }
  void set_data_digest_mismatch() {
    errors |= err_t::DATA_DIGEST_MISMATCH;
  }
  void set_data_digest_mismatch_oi() {
    errors |= err_t::DATA_DIGEST_MISMATCH_OI;
  }
  void set_size_mismatch() {
    errors |= err_t::SIZE_MISMATCH;
  }
  void set_attr_missing() {
    errors |= err_t::ATTR_MISMATCH;
  }
  void set_attr_mismatch() {
    errors |= err_t::ATTR_MISMATCH;
  }
  void set_attr_unexpected() {
    errors |= err_t::ATTR_MISMATCH;
  }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bp);
};

WRITE_CLASS_ENCODER(shard_info_wrapper)

namespace librados {
  inline void decode(librados::shard_info_t& shard,
		     bufferlist::iterator& bp) {
    reinterpret_cast<shard_info_wrapper&>(shard).decode(bp);
  }
}

struct inconsistent_obj_wrapper : librados::inconsistent_obj_t {
  inconsistent_obj_wrapper(const hobject_t& hoid);

  void add_shard(const pg_shard_t& pgs, const shard_info_wrapper& shard);
  void set_auth_missing(const hobject_t& hoid,
                        const map<pg_shard_t, ScrubMap*>& map);
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bp);
};

WRITE_CLASS_ENCODER(inconsistent_obj_wrapper)

inline void decode(librados::inconsistent_obj_t& obj,
		   bufferlist::iterator& bp) {
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}

#endif
