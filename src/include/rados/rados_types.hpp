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
  enum : uint64_t {
    SHARD_MISSING        = 1 << 1,
    SHARD_STAT_ERR       = 1 << 2,
    SHARD_READ_ERR       = 1 << 3,
    DATA_DIGEST_MISMATCH_OI = 1 << 9,
    OMAP_DIGEST_MISMATCH_OI = 1 << 10,
    SIZE_MISMATCH_OI        = 1 << 11,
    SHARD_EC_HASH_MISMATCH  = 1 << 12,
    SHARD_EC_SIZE_MISMATCH  = 1 << 13,
    OI_ATTR_MISSING         = 1 << 14,
    OI_ATTR_CORRUPTED       = 1 << 15,
    SS_ATTR_MISSING         = 1 << 16,
    SS_ATTR_CORRUPTED       = 1 << 17
    // When adding more here add to either SHALLOW_ERRORS or DEEP_ERRORS
  };
  uint64_t errors = 0;
  static constexpr uint64_t SHALLOW_ERRORS = SHARD_MISSING|SHARD_STAT_ERR|SIZE_MISMATCH_OI|OI_ATTR_MISSING|OI_ATTR_CORRUPTED|SS_ATTR_MISSING|SS_ATTR_CORRUPTED;
  static constexpr uint64_t DEEP_ERRORS = SHARD_READ_ERR|DATA_DIGEST_MISMATCH_OI|OMAP_DIGEST_MISMATCH_OI|SHARD_EC_HASH_MISMATCH|SHARD_EC_SIZE_MISMATCH;
  bool has_shard_missing() const {
    return errors & SHARD_MISSING;
  }
  bool has_stat_error() const {
    return errors & SHARD_STAT_ERR;
  }
  bool has_read_error() const {
    return errors & SHARD_READ_ERR;
  }
  bool has_data_digest_mismatch_oi() const {
    return errors & DATA_DIGEST_MISMATCH_OI;
  }
  bool has_omap_digest_mismatch_oi() const {
    return errors & OMAP_DIGEST_MISMATCH_OI;
  }
  bool has_size_mismatch_oi() const {
    return errors & SIZE_MISMATCH_OI;
  }
  bool has_ec_hash_error() const {
    return errors & SHARD_EC_HASH_MISMATCH;
  }
  bool has_ec_size_error() const {
    return errors & SHARD_EC_SIZE_MISMATCH;
  }
  bool has_oi_attr_missing() const {
    return errors & OI_ATTR_MISSING;
  }
  bool has_oi_attr_corrupted() const {
    return errors & OI_ATTR_CORRUPTED;
  }
  bool has_ss_attr_missing() const {
    return errors & SS_ATTR_MISSING;
  }
  bool has_ss_attr_corrupted() const {
    return errors & SS_ATTR_CORRUPTED;
  }
  bool has_shallow_errors() const {
    return errors & SHALLOW_ERRORS;
  }
  bool has_deep_errors() const {
    return errors & DEEP_ERRORS;
  }
};

struct shard_info_t : err_t {
  std::map<std::string, ceph::bufferlist> attrs;
  uint64_t size = -1;
  bool omap_digest_present = false;
  uint32_t omap_digest = 0;
  bool data_digest_present = false;
  uint32_t data_digest = 0;
  bool selected_oi = false;
};

struct osd_shard_t {
  int32_t osd;
  int8_t shard;
};

inline bool operator<(const osd_shard_t &lhs, const osd_shard_t &rhs) {
  if (lhs.osd < rhs.osd)
    return true;
  else if (lhs.osd > rhs.osd)
    return false;
  else
    return lhs.shard < rhs.shard;
}

struct obj_err_t {
  enum : uint64_t {
    OBJECT_INFO_INCONSISTENCY   = 1 << 1,
    // XXX: Can an older rados binary work if these bits stay the same?
    DATA_DIGEST_MISMATCH = 1 << 4,
    OMAP_DIGEST_MISMATCH = 1 << 5,
    SIZE_MISMATCH        = 1 << 6,
    ATTR_VALUE_MISMATCH  = 1 << 7,
    ATTR_NAME_MISMATCH    = 1 << 8,
    // When adding more here add to either SHALLOW_ERRORS or DEEP_ERRORS
  };
  uint64_t errors = 0;
  static constexpr uint64_t SHALLOW_ERRORS = OBJECT_INFO_INCONSISTENCY|SIZE_MISMATCH|ATTR_VALUE_MISMATCH|ATTR_NAME_MISMATCH;
  static constexpr uint64_t DEEP_ERRORS = DATA_DIGEST_MISMATCH|OMAP_DIGEST_MISMATCH;
  bool has_object_info_inconsistency() const {
    return errors & OBJECT_INFO_INCONSISTENCY;
  }
  bool has_data_digest_mismatch() const {
    return errors & DATA_DIGEST_MISMATCH;
  }
  bool has_omap_digest_mismatch() const {
    return errors & OMAP_DIGEST_MISMATCH;
  }
  bool has_size_mismatch() const {
    return errors & SIZE_MISMATCH;
  }
  bool has_attr_value_mismatch() const {
    return errors & ATTR_VALUE_MISMATCH;
  }
  bool has_attr_name_mismatch() const {
    return errors & ATTR_NAME_MISMATCH;
  }
  bool has_shallow_errors() const {
    return errors & SHALLOW_ERRORS;
  }
  bool has_deep_errors() const {
    return errors & DEEP_ERRORS;
  }
};

struct inconsistent_obj_t : obj_err_t {
  inconsistent_obj_t() = default;
  inconsistent_obj_t(const object_id_t& object)
    : object{object}, version(0)
  {}
  object_id_t object;
  uint64_t version;  // XXX: Redundant with object info attr
  std::map<osd_shard_t, shard_info_t> shards;
  err_t union_shards;
};

struct inconsistent_snapset_t {
  inconsistent_snapset_t() = default;
  inconsistent_snapset_t(const object_id_t& head)
    : object{head}
  {}
  enum {
    SNAPSET_MISSING = 1 << 0,
    SNAPSET_CORRUPTED = 1 << 1,
    CLONE_MISSING  = 1 << 2,
    SNAP_MISMATCH  = 1 << 3,
    HEAD_MISMATCH  = 1 << 4,
    HEADLESS_CLONE = 1 << 5,
    SIZE_MISMATCH  = 1 << 6,
    OI_MISSING   = 1 << 7,
    OI_CORRUPTED = 1 << 8,
    EXTRA_CLONES = 1 << 9,
  };
  uint64_t errors = 0;
  object_id_t object;
  // Extra clones
  std::vector<snap_t> clones;
  std::vector<snap_t> missing;

  bool ss_attr_missing() const {
    return errors & SNAPSET_MISSING;
  }
  bool ss_attr_corrupted() const {
    return errors & SNAPSET_CORRUPTED;
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
  bool oi_attr_missing() const {
    return errors & OI_MISSING;
  }
  bool oi_attr_corrupted() const {
    return errors & OI_CORRUPTED;
  }
  bool extra_clones() const {
    return errors & EXTRA_CLONES;
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
