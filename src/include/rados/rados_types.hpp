#ifndef CEPH_RADOS_TYPES_HPP
#define CEPH_RADOS_TYPES_HPP

#include <map>
#include <utility>
#include <vector>
#include <stdint.h>
#include <string>

#include "buffer.h"
#include "rados_types.h"

namespace librados {

typedef uint64_t snap_t;

enum {
  SNAP_HEAD = (uint64_t)(-2),
  SNAP_DIR = (uint64_t)(-1)
};

struct clone_info_t {
  snap_t cloneid;
  std::vector<snap_t> snaps;          // ascending
  std::vector< std::pair<uint64_t,uint64_t> > overlap;  // with next newest
  uint64_t size;
  clone_info_t() : cloneid(0), size(0) {}
};

struct snap_set_t {
  std::vector<clone_info_t> clones;   // ascending
  snap_t seq;   // newest snapid seen by the object
  snap_set_t() : seq(0) {}
};

struct object_id_t {
  std::string name;
  std::string nspace;
  std::string locator;
  snap_t snap = 0;
  object_id_t() = default;
  object_id_t(const std::string& name,
              const std::string& nspace,
              const std::string& locator,
              snap_t snap)
    : name(name),
      nspace(nspace),
      locator(locator),
      snap(snap)
  {}
};

struct err_t {
  enum {
    ATTR_UNEXPECTED      = 1 << 0,
    SHARD_MISSING        = 1 << 1,
    SHARD_STAT_ERR       = 1 << 2,
    SHARD_READ_ERR       = 1 << 3,
    DATA_DIGEST_MISMATCH = 1 << 4,
    OMAP_DIGEST_MISMATCH = 1 << 5,
    SIZE_MISMATCH        = 1 << 6,
    ATTR_MISMATCH        = 1 << 7,
    ATTR_MISSING         = 1 << 8,
    DATA_DIGEST_MISMATCH_OI = 1 << 9,
    OMAP_DIGEST_MISMATCH_OI = 1 << 10,
    SIZE_MISMATCH_OI        = 1 << 11,
  };
  uint64_t errors = 0;
  bool has_attr_unexpected() const {
    return errors & ATTR_UNEXPECTED;
  }
  bool has_shard_missing() const {
    return errors & SHARD_MISSING;
  }
  bool has_stat_error() const {
    return errors & SHARD_STAT_ERR;
  }
  bool has_read_error() const {
    return errors & SHARD_READ_ERR;
  }
  bool has_data_digest_mismatch() const {
    return errors & DATA_DIGEST_MISMATCH;
  }
  bool has_omap_digest_mismatch() const {
    return errors & OMAP_DIGEST_MISMATCH;
  }
  // deep error
  bool has_data_digest_mismatch_oi() const {
    return errors & DATA_DIGEST_MISMATCH_OI;
  }
  // deep error
  bool has_omap_digest_mismatch_oi() const {
    return errors & OMAP_DIGEST_MISMATCH_OI;
  }
  bool has_size_mismatch() const {
    return errors & SIZE_MISMATCH;
  }
  bool has_size_mismatch_oi() const {
    return errors & SIZE_MISMATCH_OI;
  }
  bool has_attr_mismatch() const {
    return errors & ATTR_MISMATCH;
  }
  bool has_attr_missing() const {
    return errors & ATTR_MISSING;
  }
};

struct shard_info_t : err_t {
  std::map<std::string, ceph::bufferlist> attrs;
  uint64_t size = -1;
  bool omap_digest_present = false;
  uint32_t omap_digest = 0;
  bool data_digest_present = false;
  uint32_t data_digest = 0;
};

struct inconsistent_obj_t : err_t {
  inconsistent_obj_t() = default;
  inconsistent_obj_t(const object_id_t& object)
    : object{object}
  {}
  object_id_t object;
  // osd => shard_info
  std::map<int32_t, shard_info_t> shards;
};

struct inconsistent_snapset_t {
  inconsistent_snapset_t() = default;
  inconsistent_snapset_t(const object_id_t& head)
    : object{head}
  {}
  enum {
    ATTR_MISSING   = 1 << 0,
    ATTR_CORRUPTED = 1 << 1,
    CLONE_MISSING  = 1 << 2,
    SNAP_MISMATCH  = 1 << 3,
    HEAD_MISMATCH  = 1 << 4,
    HEADLESS_CLONE = 1 << 5,
    SIZE_MISMATCH  = 1 << 6,
  };
  uint64_t errors = 0;
  object_id_t object;
  std::vector<snap_t> clones;
  std::vector<snap_t> missing;

  bool ss_attr_missing() const {
    return errors & ATTR_MISSING;
  }
  bool ss_attr_corrupted() const {
    return errors & ATTR_CORRUPTED;
  }
  bool clone_missing() const  {
    return errors & CLONE_MISSING;
  }
  bool snapset_mismatch() const {
    return errors & SNAP_MISMATCH;
  }
  bool head_mismatch() const {
    return errors & HEAD_MISMATCH;
  }
  bool headless() const {
    return errors & HEADLESS_CLONE;
  }
  bool size_mismatch() const {
    return errors & SIZE_MISMATCH;
  }
};

/**
 * @var all_nspaces
 * Pass as nspace argument to IoCtx::set_namespace()
 * before calling nobjects_begin() to iterate
 * through all objects in all namespaces.
 */
const std::string all_nspaces(LIBRADOS_ALL_NSPACES);

}
#endif
