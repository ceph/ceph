// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * include files which can only be compiled in radosgw or OSD
 * contexts (e.g., rgw_sal.h, rgw_common.h) */

#pragma once

#include <fmt/format.h>

#include "rgw_pool_types.h"
#include "rgw_user_types.h"
#include "rgw_placement_types.h"

#include "common/dout.h"
#include "common/Formatter.h"

struct cls_user_bucket;

struct rgw_bucket_key {
  std::string tenant;
  std::string name;
  std::string bucket_id;

  rgw_bucket_key(const std::string& _tenant,
                 const std::string& _name,
                 const std::string& _bucket_id) : tenant(_tenant),
                                                  name(_name),
                                                  bucket_id(_bucket_id) {}
  rgw_bucket_key(const std::string& _tenant,
                 const std::string& _name) : tenant(_tenant),
                                             name(_name) {}
};

struct rgw_bucket {
  std::string tenant;
  std::string name;
  std::string marker;
  std::string bucket_id;
  rgw_data_placement_target explicit_placement;

  rgw_bucket() { }
  // cppcheck-suppress noExplicitConstructor
  explicit rgw_bucket(const rgw_user& u, const cls_user_bucket& b);

  rgw_bucket(const std::string& _tenant,
	     const std::string& _name,
	     const std::string& _bucket_id) : tenant(_tenant),
                                              name(_name),
                                              bucket_id(_bucket_id) {}
  rgw_bucket(const rgw_bucket_key& bk) : tenant(bk.tenant),
                                         name(bk.name),
                                         bucket_id(bk.bucket_id) {}
  rgw_bucket(const rgw_bucket&) = default;
  rgw_bucket(rgw_bucket&&) = default;

  bool match(const rgw_bucket& b) const {
    return (tenant == b.tenant &&
	    name == b.name &&
	    (bucket_id == b.bucket_id ||
	     bucket_id.empty() ||
	     b.bucket_id.empty()));
  }

  void convert(cls_user_bucket *b) const;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(10, 10, bl);
    encode(name, bl);
    encode(marker, bl);
    encode(bucket_id, bl);
    encode(tenant, bl);
    bool encode_explicit = !explicit_placement.data_pool.empty();
    encode(encode_explicit, bl);
    if (encode_explicit) {
      encode(explicit_placement.data_pool, bl);
      encode(explicit_placement.data_extra_pool, bl);
      encode(explicit_placement.index_pool, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(10, 3, 3, bl);
    decode(name, bl);
    if (struct_v < 10) {
      decode(explicit_placement.data_pool.name, bl);
    }
    if (struct_v >= 2) {
      decode(marker, bl);
      if (struct_v <= 3) {
        uint64_t id;
        decode(id, bl);
        char buf[16];
        snprintf(buf, sizeof(buf), "%" PRIu64, id);
        bucket_id = buf;
      } else {
        decode(bucket_id, bl);
      }
    }
    if (struct_v < 10) {
      if (struct_v >= 5) {
        decode(explicit_placement.index_pool.name, bl);
      } else {
        explicit_placement.index_pool = explicit_placement.data_pool;
      }
      if (struct_v >= 7) {
        decode(explicit_placement.data_extra_pool.name, bl);
      }
    }
    if (struct_v >= 8) {
      decode(tenant, bl);
    }
    if (struct_v >= 10) {
      bool decode_explicit = !explicit_placement.data_pool.empty();
      decode(decode_explicit, bl);
      if (decode_explicit) {
        decode(explicit_placement.data_pool, bl);
        decode(explicit_placement.data_extra_pool, bl);
        decode(explicit_placement.index_pool, bl);
      }
    }
    DECODE_FINISH(bl);
  }

  std::string get_namespaced_name() const {
    if (tenant.empty()) {
      return name;
    }
    return tenant + std::string("/") + name;
  }

  void update_bucket_id(const std::string& new_bucket_id) {
    bucket_id = new_bucket_id;
  }

  // format a key for the bucket/instance. pass delim=0 to skip a field
  std::string get_key(char tenant_delim = '/',
                      char id_delim = ':',
                      size_t reserve = 0) const;

  const rgw_pool& get_data_extra_pool() const {
    return explicit_placement.get_data_extra_pool();
  }

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<rgw_bucket*>& o);

  rgw_bucket& operator=(const rgw_bucket&) = default;

  bool operator<(const rgw_bucket& b) const {
    if (tenant < b.tenant) {
      return true;
    } else if (tenant > b.tenant) {
      return false;
    }

    if (name < b.name) {
      return true;
    } else if (name > b.name) {
      return false;
    }

    return (bucket_id < b.bucket_id);
  }

  bool operator==(const rgw_bucket& b) const {
    return (tenant == b.tenant) && (name == b.name) && \
           (bucket_id == b.bucket_id);
  }
  bool operator!=(const rgw_bucket& b) const {
    return (tenant != b.tenant) || (name != b.name) ||
           (bucket_id != b.bucket_id);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket)

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket &b) {
  out << b.tenant << ":" << b.name << "[" << b.bucket_id << "])";
  return out;
}

struct rgw_bucket_placement {
  rgw_placement_rule placement_rule;
  rgw_bucket bucket;

  void dump(Formatter *f) const;
}; /* rgw_bucket_placement */

struct rgw_bucket_shard {
  rgw_bucket bucket;
  int shard_id;

  rgw_bucket_shard() : shard_id(-1) {}
  rgw_bucket_shard(const rgw_bucket& _b, int _sid) : bucket(_b), shard_id(_sid) {}

  std::string get_key(char tenant_delim = '/', char id_delim = ':',
                      char shard_delim = ':',
                      size_t reserve = 0) const;

  bool operator<(const rgw_bucket_shard& b) const {
    if (bucket < b.bucket) {
      return true;
    }
    if (b.bucket < bucket) {
      return false;
    }
    return shard_id < b.shard_id;
  }

  bool operator==(const rgw_bucket_shard& b) const {
    return (bucket == b.bucket &&
            shard_id == b.shard_id);
  }
}; /* rgw_bucket_shard */

void encode(const rgw_bucket_shard& b, bufferlist& bl, uint64_t f=0);
void decode(rgw_bucket_shard& b, bufferlist::const_iterator& bl);

inline std::ostream& operator<<(std::ostream& out, const rgw_bucket_shard& bs) {
  if (bs.shard_id <= 0) {
    return out << bs.bucket;
  }

  return out << bs.bucket << ":" << bs.shard_id;
}
