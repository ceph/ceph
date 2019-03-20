// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for
 * use with rbd.
 *
 * Most of these deal with the rbd header object. Methods prefixed
 * with old_ deal with the original rbd design, in which clients read
 * and interpreted the header object directly.
 *
 * The new format is meant to be opaque to clients - all their
 * interactions with non-data objects should go through this
 * class. The OSD class interface leaves the class to implement its
 * own argument and payload serialization/deserialization, so for ease
 * of implementation we use the existing ceph encoding/decoding
 * methods. Something like json might be preferable, but the rbd
 * kernel module has to be able to understand format as well. The
 * datatypes exposed to the clients are strings, unsigned integers,
 * and vectors of those types. The on-wire format can be found in
 * src/include/encoding.h.
 *
 * The methods for interacting with the new format document their
 * parameters as the client sees them - it would be silly to mention
 * in each one that they take an input and an output bufferlist.
 */
#include "include/types.h"

#include <algorithm>
#include <errno.h>
#include <sstream>

#include "include/uuid.h"
#include "common/bit_vector.hpp"
#include "common/errno.h"
#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include "include/rbd_types.h"
#include "include/rbd/object_map_types.h"

#include "cls/rbd/cls_rbd.h"
#include "cls/rbd/cls_rbd_types.h"


/*
 * Object keys:
 *
 * <partial list>
 *
 * stripe_unit: size in bytes of the stripe unit.  if not present,
 *   the stripe unit is assumed to match the object size (1 << order).
 *
 * stripe_count: number of objects to stripe over before looping back.
 *   if not present or 1, striping is disabled.  this is the default.
 *
 */

CLS_VER(2,0)
CLS_NAME(rbd)

#define RBD_MAX_KEYS_READ 64
#define RBD_SNAP_KEY_PREFIX "snapshot_"
#define RBD_SNAP_CHILDREN_KEY_PREFIX "snap_children_"
#define RBD_DIR_ID_KEY_PREFIX "id_"
#define RBD_DIR_NAME_KEY_PREFIX "name_"
#define RBD_METADATA_KEY_PREFIX "metadata_"

namespace {

uint64_t get_encode_features(cls_method_context_t hctx) {
  uint64_t features = 0;
  int8_t require_osd_release = cls_get_required_osd_release(hctx);
  if (require_osd_release >= CEPH_RELEASE_NAUTILUS) {
    features |= CEPH_FEATURE_SERVER_NAUTILUS;
  }
  return features;
}

bool calc_sparse_extent(const bufferptr &bp, size_t sparse_size,
                        uint64_t length, size_t *write_offset,
                        size_t *write_length, size_t *offset) {
  size_t extent_size;
  if (*offset + sparse_size > length) {
    extent_size = length - *offset;
  } else {
    extent_size = sparse_size;
  }

  bufferptr extent(bp, *offset, extent_size);
  *offset += extent_size;

  bool extent_is_zero = extent.is_zero();
  if (!extent_is_zero) {
    *write_length += extent_size;
  }
  if (extent_is_zero && *write_length == 0) {
    *write_offset += extent_size;
  }

  if ((extent_is_zero || *offset == length) && *write_length != 0) {
    return true;
  }
  return false;
}

} // anonymous namespace

static int snap_read_header(cls_method_context_t hctx, bufferlist& bl)
{
  unsigned snap_count = 0;
  uint64_t snap_names_len = 0;
  struct rbd_obj_header_ondisk *header;

  CLS_LOG(20, "snapshots_list");

  while (1) {
    int len = sizeof(*header) +
      snap_count * sizeof(struct rbd_obj_snap_ondisk) +
      snap_names_len;

    int rc = cls_cxx_read(hctx, 0, len, &bl);
    if (rc < 0)
      return rc;

    if (bl.length() < sizeof(*header))
      return -EINVAL;

    header = (struct rbd_obj_header_ondisk *)bl.c_str();
    ceph_assert(header);

    if ((snap_count != header->snap_count) ||
        (snap_names_len != header->snap_names_len)) {
      snap_count = header->snap_count;
      snap_names_len = header->snap_names_len;
      bl.clear();
      continue;
    }
    break;
  }

  return 0;
}

static void key_from_snap_id(snapid_t snap_id, string *out)
{
  ostringstream oss;
  oss << RBD_SNAP_KEY_PREFIX
      << std::setw(16) << std::setfill('0') << std::hex << snap_id;
  *out = oss.str();
}

static snapid_t snap_id_from_key(const string &key) {
  istringstream iss(key);
  uint64_t id;
  iss.ignore(strlen(RBD_SNAP_KEY_PREFIX)) >> std::hex >> id;
  return id;
}

template<typename T>
static int read_key(cls_method_context_t hctx, const string &key, T *out)
{
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading omap key %s: %s", key.c_str(), cpp_strerror(r).c_str());
    }
    return r;
  }

  try {
    auto it = bl.cbegin();
    decode(*out, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding %s", key.c_str());
    return -EIO;
  }

  return 0;
}

template <typename T>
static int write_key(cls_method_context_t hctx, const string &key, const T &t) {
  bufferlist bl;
  encode(t, bl);

  int r = cls_cxx_map_set_val(hctx, key, &bl);
  if (r < 0) {
    CLS_ERR("failed to set omap key: %s", key.c_str());
    return r;
  }
  return 0;
}

template <typename T>
static int write_key(cls_method_context_t hctx, const string &key, const T &t,
                     uint64_t features) {
  bufferlist bl;
  encode(t, bl, features);

  int r = cls_cxx_map_set_val(hctx, key, &bl);
  if (r < 0) {
    CLS_ERR("failed to set omap key: %s", key.c_str());
    return r;
  }
  return 0;
}

static int remove_key(cls_method_context_t hctx, const string &key) {
  int r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to remove key: %s", key.c_str());
      return r;
  }
  return 0;
}

static bool is_valid_id(const string &id) {
  if (!id.size())
    return false;
  for (size_t i = 0; i < id.size(); ++i) {
    if (!isalnum(id[i])) {
      return false;
    }
  }
  return true;
}

/**
 * verify that the header object exists
 *
 * @return 0 if the object exists, -ENOENT if it does not, or other error
 */
static int check_exists(cls_method_context_t hctx)
{
  uint64_t size;
  time_t mtime;
  return cls_cxx_stat(hctx, &size, &mtime);
}

namespace image {

/**
 * check that given feature(s) are set
 *
 * @param hctx context
 * @param need features needed
 * @return 0 if features are set, negative error (like ENOEXEC) otherwise
 */
int require_feature(cls_method_context_t hctx, uint64_t need)
{
  uint64_t features;
  int r = read_key(hctx, "features", &features);
  if (r == -ENOENT)   // this implies it's an old-style image with no features
    return -ENOEXEC;
  if (r < 0)
    return r;
  if ((features & need) != need) {
    CLS_LOG(10, "require_feature missing feature %llx, have %llx",
            (unsigned long long)need, (unsigned long long)features);
    return -ENOEXEC;
  }
  return 0;
}

std::string snap_children_key_from_snap_id(snapid_t snap_id)
{
  ostringstream oss;
  oss << RBD_SNAP_CHILDREN_KEY_PREFIX
      << std::setw(16) << std::setfill('0') << std::hex << snap_id;
  return oss.str();
}

int set_op_features(cls_method_context_t hctx, uint64_t op_features,
                    uint64_t mask) {
  uint64_t orig_features;
  int r = read_key(hctx, "features", &orig_features);
  if (r < 0) {
    CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  uint64_t orig_op_features = 0;
  r = read_key(hctx, "op_features", &orig_op_features);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("Could not read op features off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  op_features = (orig_op_features & ~mask) | (op_features & mask);
  CLS_LOG(10, "op_features=%" PRIu64 " orig_op_features=%" PRIu64,
          op_features, orig_op_features);
  if (op_features == orig_op_features) {
    return 0;
  }

  uint64_t features = orig_features;
  if (op_features == 0ULL) {
    features &= ~RBD_FEATURE_OPERATIONS;

    r = cls_cxx_map_remove_key(hctx, "op_features");
    if (r == -ENOENT) {
      r = 0;
    }
  } else {
    features |= RBD_FEATURE_OPERATIONS;

    bufferlist bl;
    encode(op_features, bl);
    r = cls_cxx_map_set_val(hctx, "op_features", &bl);
  }

  if (r < 0) {
    CLS_ERR("error updating op features: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (features != orig_features) {
    bufferlist bl;
    encode(features, bl);
    r = cls_cxx_map_set_val(hctx, "features", &bl);
    if (r < 0) {
      CLS_ERR("error updating features: %s", cpp_strerror(r).c_str());
      return r;
    }
  }

  return 0;
}

int set_migration(cls_method_context_t hctx,
                  const cls::rbd::MigrationSpec &migration_spec, bool init) {
  if (init) {
    bufferlist bl;
    int r = cls_cxx_map_get_val(hctx, "migration", &bl);
    if (r != -ENOENT) {
      if (r == 0) {
        CLS_LOG(10, "migration already set");
        return -EEXIST;
      }
      CLS_ERR("failed to read migration off disk: %s", cpp_strerror(r).c_str());
      return r;
    }

    uint64_t features = 0;
    r = read_key(hctx, "features", &features);
    if (r == -ENOENT) {
      CLS_LOG(20, "no features, assuming v1 format");
      bufferlist header;
      r = cls_cxx_read(hctx, 0, sizeof(RBD_HEADER_TEXT), &header);
      if (r < 0) {
        CLS_ERR("failed to read v1 header: %s", cpp_strerror(r).c_str());
        return r;
      }
      if (header.length() != sizeof(RBD_HEADER_TEXT)) {
        CLS_ERR("unrecognized v1 header format");
        return -ENXIO;
      }
      if (memcmp(RBD_HEADER_TEXT, header.c_str(), header.length()) != 0) {
        if (memcmp(RBD_MIGRATE_HEADER_TEXT, header.c_str(),
                   header.length()) == 0) {
          CLS_LOG(10, "migration already set");
          return -EEXIST;
        } else {
          CLS_ERR("unrecognized v1 header format");
          return -ENXIO;
        }
      }
      if (migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_SRC) {
        CLS_LOG(10, "v1 format image can only be migration source");
        return -EINVAL;
      }

      header.clear();
      header.append(RBD_MIGRATE_HEADER_TEXT);
      r = cls_cxx_write(hctx, 0, header.length(), &header);
      if (r < 0) {
        CLS_ERR("error updating v1 header: %s", cpp_strerror(r).c_str());
        return r;
      }
    } else if (r < 0) {
      CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
      return r;
    } else if ((features & RBD_FEATURE_MIGRATING) != 0ULL) {
      if (migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_DST) {
        CLS_LOG(10, "migrating feature already set");
        return -EEXIST;
      }
    } else {
      features |= RBD_FEATURE_MIGRATING;
      bl.clear();
      encode(features, bl);
      r = cls_cxx_map_set_val(hctx, "features", &bl);
      if (r < 0) {
        CLS_ERR("error updating features: %s", cpp_strerror(r).c_str());
        return r;
      }
    }
  }

  bufferlist bl;
  encode(migration_spec, bl);
  int r = cls_cxx_map_set_val(hctx, "migration", &bl);
  if (r < 0) {
    CLS_ERR("error setting migration: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

int read_migration(cls_method_context_t hctx,
                   cls::rbd::MigrationSpec *migration_spec) {
  uint64_t features = 0;
  int r = read_key(hctx, "features", &features);
  if (r == -ENOENT) {
    CLS_LOG(20, "no features, assuming v1 format");
    bufferlist header;
    r = cls_cxx_read(hctx, 0, sizeof(RBD_HEADER_TEXT), &header);
    if (r < 0) {
      CLS_ERR("failed to read v1 header: %s", cpp_strerror(r).c_str());
      return r;
    }
    if (header.length() != sizeof(RBD_HEADER_TEXT)) {
      CLS_ERR("unrecognized v1 header format");
      return -ENXIO;
    }
    if (memcmp(RBD_MIGRATE_HEADER_TEXT, header.c_str(), header.length()) != 0) {
      if (memcmp(RBD_HEADER_TEXT, header.c_str(), header.length()) == 0) {
        CLS_LOG(10, "migration feature not set");
        return -EINVAL;
      } else {
        CLS_ERR("unrecognized v1 header format");
        return -ENXIO;
      }
    }
    if (migration_spec->header_type != cls::rbd::MIGRATION_HEADER_TYPE_SRC) {
      CLS_LOG(10, "v1 format image can only be migration source");
      return -EINVAL;
    }
  } else if (r < 0) {
    CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
    return r;
  } else if ((features & RBD_FEATURE_MIGRATING) == 0ULL) {
    CLS_LOG(10, "migration feature not set");
    return -EINVAL;
  }

  r = read_key(hctx, "migration", migration_spec);
  if (r < 0) {
    CLS_ERR("failed to read migration off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

int remove_migration(cls_method_context_t hctx) {
  int r = remove_key(hctx, "migration");
  if (r < 0) {
    return r;
  }

  uint64_t features = 0;
  r = read_key(hctx, "features", &features);
  if (r == -ENOENT) {
    CLS_LOG(20, "no features, assuming v1 format");
    bufferlist header;
    r = cls_cxx_read(hctx, 0, sizeof(RBD_MIGRATE_HEADER_TEXT), &header);
    if (header.length() != sizeof(RBD_MIGRATE_HEADER_TEXT)) {
      CLS_ERR("unrecognized v1 header format");
      return -ENXIO;
    }
    if (memcmp(RBD_MIGRATE_HEADER_TEXT, header.c_str(), header.length()) != 0) {
      if (memcmp(RBD_HEADER_TEXT, header.c_str(), header.length()) == 0) {
        CLS_LOG(10, "migration feature not set");
        return -EINVAL;
      } else {
        CLS_ERR("unrecognized v1 header format");
        return -ENXIO;
      }
    }
    header.clear();
    header.append(RBD_HEADER_TEXT);
    r = cls_cxx_write(hctx, 0, header.length(), &header);
    if (r < 0) {
      CLS_ERR("error updating v1 header: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else if (r < 0) {
    CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
    return r;
  } else if ((features & RBD_FEATURE_MIGRATING) == 0ULL) {
    CLS_LOG(10, "migrating feature not set");
  } else {
    features &= ~RBD_FEATURE_MIGRATING;
    bufferlist bl;
    encode(features, bl);
    r = cls_cxx_map_set_val(hctx, "features", &bl);
    if (r < 0) {
      CLS_ERR("error updating features: %s", cpp_strerror(r).c_str());
      return r;
    }
  }

  return 0;
}

namespace snapshot {

template<typename L>
int iterate(cls_method_context_t hctx, L& lambda) {
  int max_read = RBD_MAX_KEYS_READ;
  string last_read = RBD_SNAP_KEY_PREFIX;
  bool more = false;
  do {
    map<string, bufferlist> vals;
    int r = cls_cxx_map_get_vals(hctx, last_read, RBD_SNAP_KEY_PREFIX,
			         max_read, &vals, &more);
    if (r < 0) {
      return r;
    }

    cls_rbd_snap snap_meta;
    for (auto& val : vals) {
      auto iter = val.second.cbegin();
      try {
	decode(snap_meta, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("error decoding snapshot metadata for snap : %s",
	        val.first.c_str());
	return -EIO;
      }

      r = lambda(snap_meta);
      if (r < 0) {
        return r;
      }
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  } while (more);

  return 0;
}

int write(cls_method_context_t hctx, const std::string& snap_key,
          cls_rbd_snap&& snap) {
  int r;
  uint64_t encode_features = get_encode_features(hctx);
  if (snap.migrate_parent_format(encode_features)) {
    // ensure the normalized parent link exists before removing it from the
    // snapshot record
    cls_rbd_parent on_disk_parent;
    r = read_key(hctx, "parent", &on_disk_parent);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    if (!on_disk_parent.exists()) {
      on_disk_parent = snap.parent;
      on_disk_parent.head_overlap = std::nullopt;

      r = write_key(hctx, "parent", on_disk_parent, encode_features);
      if (r < 0) {
        return r;
      }
    }

    // only store the parent overlap in the snapshot
    snap.parent_overlap = snap.parent.head_overlap;
    snap.parent = {};
  }

  r = write_key(hctx, snap_key, snap, encode_features);
  if (r < 0) {
    return r;
  }
  return 0;
}

} // namespace snapshot

namespace parent {

int attach(cls_method_context_t hctx, cls_rbd_parent parent,
           bool reattach) {
  int r = check_exists(hctx);
  if (r < 0) {
    CLS_LOG(20, "cls_rbd::image::parent::attach: child doesn't exist");
    return r;
  }

  r = image::require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r < 0) {
    CLS_LOG(20, "cls_rbd::image::parent::attach: child does not support "
                "layering");
    return r;
  }

  CLS_LOG(20, "cls_rbd::image::parent::attach: pool=%" PRIi64 ", ns=%s, id=%s, "
              "snapid=%" PRIu64 ", size=%" PRIu64,
          parent.pool_id, parent.pool_namespace.c_str(),
          parent.image_id.c_str(), parent.snap_id.val,
          parent.head_overlap.value_or(0ULL));
  if (!parent.exists() || parent.head_overlap.value_or(0ULL) == 0ULL) {
    return -EINVAL;
  }

  // make sure there isn't already a parent
  cls_rbd_parent on_disk_parent;
  r = read_key(hctx, "parent", &on_disk_parent);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  auto on_disk_parent_without_overlap{on_disk_parent};
  on_disk_parent_without_overlap.head_overlap = parent.head_overlap;

  if (r == 0 &&
      (on_disk_parent.head_overlap ||
       on_disk_parent_without_overlap != parent) &&
      !reattach) {
    CLS_LOG(20, "cls_rbd::parent::attach: existing legacy parent "
                "pool=%" PRIi64 ", ns=%s, id=%s, snapid=%" PRIu64 ", "
                "overlap=%" PRIu64,
            on_disk_parent.pool_id, on_disk_parent.pool_namespace.c_str(),
            on_disk_parent.image_id.c_str(), on_disk_parent.snap_id.val,
            on_disk_parent.head_overlap.value_or(0ULL));
    return -EEXIST;
  }

  // our overlap is the min of our size and the parent's size.
  uint64_t our_size;
  r = read_key(hctx, "size", &our_size);
  if (r < 0) {
    return r;
  }

  parent.head_overlap = std::min(*parent.head_overlap, our_size);

  r = write_key(hctx, "parent", parent, get_encode_features(hctx));
  if (r < 0) {
    return r;
  }

  return 0;
}

int detach(cls_method_context_t hctx, bool legacy_api) {
  int r = check_exists(hctx);
  if (r < 0) {
    CLS_LOG(20, "cls_rbd::parent::detach: child doesn't exist");
    return r;
  }

  uint64_t features;
  r = read_key(hctx, "features", &features);
  if (r == -ENOENT || ((features & RBD_FEATURE_LAYERING) == 0)) {
    CLS_LOG(20, "cls_rbd::image::parent::detach: child does not support "
                "layering");
    return -ENOEXEC;
  } else if (r < 0) {
    return r;
  }

  cls_rbd_parent on_disk_parent;
  r = read_key(hctx, "parent", &on_disk_parent);
  if (r < 0) {
    return r;
  } else if (legacy_api && !on_disk_parent.pool_namespace.empty()) {
    return -EXDEV;
  } else if (!on_disk_parent.head_overlap) {
    return -ENOENT;
  }

  auto detach_lambda = [hctx, features](const cls_rbd_snap& snap_meta) {
    if (snap_meta.parent.pool_id != -1 || snap_meta.parent_overlap) {
      if ((features & RBD_FEATURE_DEEP_FLATTEN) != 0ULL) {
        // remove parent reference from snapshot
        cls_rbd_snap snap_meta_copy = snap_meta;
        snap_meta_copy.parent = {};
        snap_meta_copy.parent_overlap = std::nullopt;

        std::string snap_key;
        key_from_snap_id(snap_meta_copy.id, &snap_key);
        int r = snapshot::write(hctx, snap_key, std::move(snap_meta_copy));
        if (r < 0) {
          return r;
        }
      } else {
        return -EEXIST;
      }
    }
    return 0;
  };

  r = snapshot::iterate(hctx, detach_lambda);
  bool has_child_snaps = (r == -EEXIST);
  if (r < 0 && r != -EEXIST) {
    return r;
  }

  int8_t require_osd_release = cls_get_required_osd_release(hctx);
  if (has_child_snaps && require_osd_release >= CEPH_RELEASE_NAUTILUS) {
    // remove overlap from HEAD revision but keep spec for snapshots
    on_disk_parent.head_overlap = std::nullopt;
    r = write_key(hctx, "parent", on_disk_parent, get_encode_features(hctx));
    if (r < 0) {
      return r;
    }
  } else {
    r = remove_key(hctx, "parent");
    if (r < 0 && r != -ENOENT) {
      return r;
    }
  }

  if (!has_child_snaps) {
    // disable clone child op feature if no longer associated
    r = set_op_features(hctx, 0, RBD_OPERATION_FEATURE_CLONE_CHILD);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

} // namespace parent
} // namespace image

/**
 * Initialize the header with basic metadata.
 * Extra features may initialize more fields in the future.
 * Everything is stored as key/value pairs as omaps in the header object.
 *
 * If features the OSD does not understand are requested, -ENOSYS is
 * returned.
 *
 * Input:
 * @param size number of bytes in the image (uint64_t)
 * @param order bits to shift to determine the size of data objects (uint8_t)
 * @param features what optional things this image will use (uint64_t)
 * @param object_prefix a prefix for all the data objects
 * @param data_pool_id pool id where data objects is stored (int64_t)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int create(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string object_prefix;
  uint64_t features, size;
  uint8_t order;
  int64_t data_pool_id = -1;

  try {
    auto iter = in->cbegin();
    decode(size, iter);
    decode(order, iter);
    decode(features, iter);
    decode(object_prefix, iter);
    if (!iter.end()) {
      decode(data_pool_id, iter);
    }
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "create object_prefix=%s size=%llu order=%u features=%llu",
	  object_prefix.c_str(), (unsigned long long)size, order,
	  (unsigned long long)features);

  if (features & ~RBD_FEATURES_ALL) {
    return -ENOSYS;
  }

  if (!object_prefix.size()) {
    return -EINVAL;
  }

  bufferlist stored_prefixbl;
  int r = cls_cxx_map_get_val(hctx, "object_prefix", &stored_prefixbl);
  if (r != -ENOENT) {
    CLS_ERR("reading object_prefix returned %d", r);
    return -EEXIST;
  }

  bufferlist sizebl;
  bufferlist orderbl;
  bufferlist featuresbl;
  bufferlist object_prefixbl;
  bufferlist snap_seqbl;
  bufferlist timestampbl;
  uint64_t snap_seq = 0;
  utime_t timestamp = ceph_clock_now();
  encode(size, sizebl);
  encode(order, orderbl);
  encode(features, featuresbl);
  encode(object_prefix, object_prefixbl);
  encode(snap_seq, snap_seqbl);
  encode(timestamp, timestampbl);

  map<string, bufferlist> omap_vals;
  omap_vals["size"] = sizebl;
  omap_vals["order"] = orderbl;
  omap_vals["features"] = featuresbl;
  omap_vals["object_prefix"] = object_prefixbl;
  omap_vals["snap_seq"] = snap_seqbl;
  omap_vals["create_timestamp"] = timestampbl;
  omap_vals["access_timestamp"] = timestampbl;
  omap_vals["modify_timestamp"] = timestampbl;

  if ((features & RBD_FEATURE_OPERATIONS) != 0ULL) {
    CLS_ERR("Attempting to set internal feature: operations");
    return -EINVAL;
  }

  if (features & RBD_FEATURE_DATA_POOL) {
    if (data_pool_id == -1) {
      CLS_ERR("data pool not provided with feature enabled");
      return -EINVAL;
    }

    bufferlist data_pool_id_bl;
    encode(data_pool_id, data_pool_id_bl);
    omap_vals["data_pool_id"] = data_pool_id_bl;
  } else if (data_pool_id != -1) {
    CLS_ERR("data pool provided with feature disabled");
    return -EINVAL;
  }

  r = cls_cxx_map_set_vals(hctx, &omap_vals);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t) (deprecated)
 * @param read_only true if the image will be used read-only (bool)
 *
 * Output:
 * @param features list of enabled features for the given snapshot (uint64_t)
 * @param incompatible incompatible feature bits
 * @returns 0 on success, negative error code on failure
 */
int get_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bool read_only = false;

  auto iter = in->cbegin();
  try {
    uint64_t snap_id;
    decode(snap_id, iter);
    if (!iter.end()) {
      decode(read_only, iter);
    }
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_features read_only=%d", read_only);

  uint64_t features;
  int r = read_key(hctx, "features", &features);
  if (r < 0) {
    CLS_ERR("failed to read features off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  uint64_t incompatible = (read_only ? features & RBD_FEATURES_INCOMPATIBLE :
				       features & RBD_FEATURES_RW_INCOMPATIBLE);
  encode(features, *out);
  encode(incompatible, *out);
  return 0;
}

/**
 * set the image features
 *
 * Input:
 * @param features image features
 * @param mask image feature mask
 *
 * Output:
 * none
 *
 * @returns 0 on success, negative error code upon failure
 */
int set_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t features;
  uint64_t mask;
  auto iter = in->cbegin();
  try {
    decode(features, iter);
    decode(mask, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // check that features exists to make sure this is a header object
  // that was created correctly
  uint64_t orig_features = 0;
  int r = read_key(hctx, "features", &orig_features);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("Could not read image's features off disk: %s",
            cpp_strerror(r).c_str());
    return r;
  }

  if ((mask & RBD_FEATURES_INTERNAL) != 0ULL) {
    CLS_ERR("Attempting to set internal feature: %" PRIu64,
            static_cast<uint64_t>(mask & RBD_FEATURES_INTERNAL));
    return -EINVAL;
  }

  // newer clients might attempt to mask off features we don't support
  mask &= RBD_FEATURES_ALL;

  uint64_t enabled_features = features & mask;
  if ((enabled_features & RBD_FEATURES_MUTABLE) != enabled_features) {
    CLS_ERR("Attempting to enable immutable feature: %" PRIu64,
            static_cast<uint64_t>(enabled_features & ~RBD_FEATURES_MUTABLE));
    return -EINVAL;
  }

  uint64_t disabled_features = ~features & mask;
  uint64_t disable_mask = (RBD_FEATURES_MUTABLE | RBD_FEATURES_DISABLE_ONLY);
  if ((disabled_features & disable_mask) != disabled_features) {
       CLS_ERR("Attempting to disable immutable feature: %" PRIu64,
               enabled_features & ~disable_mask);
       return -EINVAL;
  }

  features = (orig_features & ~mask) | (features & mask);
  CLS_LOG(10, "set_features features=%" PRIu64 " orig_features=%" PRIu64,
          features, orig_features);

  bufferlist bl;
  encode(features, bl);
  r = cls_cxx_map_set_val(hctx, "features", &bl);
  if (r < 0) {
    CLS_ERR("error updating features: %s", cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param order bits to shift to get the size of data objects (uint8_t)
 * @param size size of the image in bytes for the given snapshot (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int get_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id, size;
  uint8_t order;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_size snap_id=%llu", (unsigned long long)snap_id);

  int r = read_key(hctx, "order", &order);
  if (r < 0) {
    CLS_ERR("failed to read the order off of disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (snap_id == CEPH_NOSNAP) {
    r = read_key(hctx, "size", &size);
    if (r < 0) {
      CLS_ERR("failed to read the image's size off of disk: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    cls_rbd_snap snap;
    string snapshot_key;
    key_from_snap_id(snap_id, &snapshot_key);
    int r = read_key(hctx, snapshot_key, &snap);
    if (r < 0)
      return r;

    size = snap.image_size;
  }

  encode(order, *out);
  encode(size, *out);

  return 0;
}

/**
 * Input:
 * @param size new capacity of the image in bytes (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int set_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;

  auto iter = in->cbegin();
  try {
    decode(size, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // check that size exists to make sure this is a header object
  // that was created correctly
  uint64_t orig_size;
  int r = read_key(hctx, "size", &orig_size);
  if (r < 0) {
    CLS_ERR("Could not read image's size off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  CLS_LOG(20, "set_size size=%llu orig_size=%llu", (unsigned long long)size,
          (unsigned long long)orig_size);

  bufferlist sizebl;
  encode(size, sizebl);
  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0) {
    CLS_ERR("error writing snapshot metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  // if we are shrinking, and have a parent, shrink our overlap with
  // the parent, too.
  if (size < orig_size) {
    cls_rbd_parent parent;
    r = read_key(hctx, "parent", &parent);
    if (r == -ENOENT)
      r = 0;
    if (r < 0)
      return r;
    if (parent.exists() && parent.head_overlap.value_or(0ULL) > size) {
      parent.head_overlap = size;
      r = write_key(hctx, "parent", parent, get_encode_features(hctx));
      if (r < 0) {
	return r;
      }
    }
  }

  return 0;
}

/**
 * get the current protection status of the specified snapshot
 *
 * Input:
 * @param snap_id (uint64_t) which snapshot to get the status of
 *
 * Output:
 * @param status (uint8_t) one of:
 * RBD_PROTECTION_STATUS_{PROTECTED, UNPROTECTED, UNPROTECTING}
 *
 * @returns 0 on success, negative error code on failure
 * @returns -EINVAL if snapid is CEPH_NOSNAP
 */
int get_protection_status(cls_method_context_t hctx, bufferlist *in,
			  bufferlist *out)
{
  snapid_t snap_id;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "get_protection_status: invalid decode");
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "get_protection_status snap_id=%llu",
         (unsigned long long)snap_id.val);

  if (snap_id == CEPH_NOSNAP)
    return -EINVAL;

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id.val, &snapshot_key);
  r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    CLS_ERR("could not read key for snapshot id %" PRIu64, snap_id.val);
    return r;
  }

  if (snap.protection_status >= RBD_PROTECTION_STATUS_LAST) {
    CLS_ERR("invalid protection status for snap id %llu: %u",
	    (unsigned long long)snap_id.val, snap.protection_status);
    return -EIO;
  }

  encode(snap.protection_status, *out);
  return 0;
}

/**
 * set the proctection status of a snapshot
 *
 * Input:
 * @param snapid (uint64_t) which snapshot to set the status of
 * @param status (uint8_t) one of:
 * RBD_PROTECTION_STATUS_{PROTECTED, UNPROTECTED, UNPROTECTING}
 *
 * @returns 0 on success, negative error code on failure
 * @returns -EINVAL if snapid is CEPH_NOSNAP
 */
int set_protection_status(cls_method_context_t hctx, bufferlist *in,
			  bufferlist *out)
{
  snapid_t snap_id;
  uint8_t status;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
    decode(status, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "set_protection_status: invalid decode");
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0)
    return r;

  r = image::require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r < 0) {
    CLS_LOG(20, "image does not support layering");
    return r;
  }

  CLS_LOG(20, "set_protection_status snapid=%llu status=%u",
	  (unsigned long long)snap_id.val, status);

  if (snap_id == CEPH_NOSNAP)
    return -EINVAL;

  if (status >= RBD_PROTECTION_STATUS_LAST) {
    CLS_LOG(10, "invalid protection status for snap id %llu: %u",
	    (unsigned long long)snap_id.val, status);
    return -EINVAL;
  }

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id.val, &snapshot_key);
  r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    CLS_ERR("could not read key for snapshot id %" PRIu64, snap_id.val);
    return r;
  }

  snap.protection_status = status;
  r = image::snapshot::write(hctx, snapshot_key, std::move(snap));
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * get striping parameters
 *
 * Input:
 * none
 *
 * Output:
 * @param stripe unit (bytes)
 * @param stripe count (num objects)
 *
 * @returns 0 on success
 */
int get_stripe_unit_count(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "get_stripe_unit_count");

  r = image::require_feature(hctx, RBD_FEATURE_STRIPINGV2);
  if (r < 0)
    return r;

  uint64_t stripe_unit = 0, stripe_count = 0;
  r = read_key(hctx, "stripe_unit", &stripe_unit);
  if (r == -ENOENT) {
    // default to object size
    uint8_t order;
    r = read_key(hctx, "order", &order);
    if (r < 0) {
      CLS_ERR("failed to read the order off of disk: %s", cpp_strerror(r).c_str());
      return -EIO;
    }
    stripe_unit = 1ull << order;
  }
  if (r < 0)
    return r;
  r = read_key(hctx, "stripe_count", &stripe_count);
  if (r == -ENOENT) {
    // default to 1
    stripe_count = 1;
    r = 0;
  }
  if (r < 0)
    return r;

  encode(stripe_unit, *out);
  encode(stripe_count, *out);
  return 0;
}

/**
 * set striping parameters
 *
 * Input:
 * @param stripe unit (bytes)
 * @param stripe count (num objects)
 *
 * @returns 0 on success
 */
int set_stripe_unit_count(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t stripe_unit, stripe_count;

  auto iter = in->cbegin();
  try {
    decode(stripe_unit, iter);
    decode(stripe_count, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "set_stripe_unit_count: invalid decode");
    return -EINVAL;
  }

  if (!stripe_count || !stripe_unit)
    return -EINVAL;

  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "set_stripe_unit_count");

  r = image::require_feature(hctx, RBD_FEATURE_STRIPINGV2);
  if (r < 0)
    return r;

  uint8_t order;
  r = read_key(hctx, "order", &order);
  if (r < 0) {
    CLS_ERR("failed to read the order off of disk: %s", cpp_strerror(r).c_str());
    return r;
  }
  if ((1ull << order) % stripe_unit || stripe_unit > (1ull << order)) {
    CLS_ERR("stripe unit %llu is not a factor of the object size %llu",
            (unsigned long long)stripe_unit, 1ull << order);
    return -EINVAL;
  }

  bufferlist bl, bl2;
  encode(stripe_unit, bl);
  r = cls_cxx_map_set_val(hctx, "stripe_unit", &bl);
  if (r < 0) {
    CLS_ERR("error writing stripe_unit metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  encode(stripe_count, bl2);
  r = cls_cxx_map_set_val(hctx, "stripe_count", &bl2);
  if (r < 0) {
    CLS_ERR("error writing stripe_count metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

int get_create_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_create_timestamp");

  utime_t timestamp;
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, "create_timestamp", &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading create_timestamp: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    try {
      auto it = bl.cbegin();
      decode(timestamp, it);
    } catch (const buffer::error &err) {
      CLS_ERR("could not decode create_timestamp");
      return -EIO;
    }
  }

  encode(timestamp, *out);
  return 0;
}

/**
 * get the image access timestamp
 *
 * Input:
 * @param none
 *
 * Output:
 * @param timestamp the image access timestamp
 *
 * @returns 0 on success, negative error code upon failure
 */
int get_access_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_access_timestamp");

  utime_t timestamp;
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, "access_timestamp", &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading access_timestamp: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    try {
      auto it = bl.cbegin();
      decode(timestamp, it);
    } catch (const buffer::error &err) {
      CLS_ERR("could not decode access_timestamp");
      return -EIO;
    }
  }

  encode(timestamp, *out);
  return 0;
}

/**
 * get the image modify timestamp
 *
 * Input:
 * @param none
 *
 * Output:
 * @param timestamp the image modify timestamp
 *
 * @returns 0 on success, negative error code upon failure
 */
int get_modify_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_modify_timestamp");

  utime_t timestamp;
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, "modify_timestamp", &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading modify_timestamp: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    try {
      auto it = bl.cbegin();
      decode(timestamp, it);
    } catch (const buffer::error &err) {
      CLS_ERR("could not decode modify_timestamp");
      return -EIO;
    }
  }

  encode(timestamp, *out);
  return 0;
}


/**
 * get the image flags
 *
 * Input:
 * @param snap_id which snapshot to query, to CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param flags image flags
 *
 * @returns 0 on success, negative error code upon failure
 */
int get_flags(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;
  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_flags snap_id=%llu", (unsigned long long)snap_id);

  uint64_t flags = 0;
  if (snap_id == CEPH_NOSNAP) {
    int r = read_key(hctx, "flags", &flags);
    if (r < 0 && r != -ENOENT) {
      CLS_ERR("failed to read flags off disk: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    cls_rbd_snap snap;
    string snapshot_key;
    key_from_snap_id(snap_id, &snapshot_key);
    int r = read_key(hctx, snapshot_key, &snap);
    if (r < 0) {
      return r;
    }
    flags = snap.flags;
  }

  encode(flags, *out);
  return 0;
}

/**
 * set the image flags
 *
 * Input:
 * @param flags image flags
 * @param mask image flag mask
 * @param snap_id which snapshot to update, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * none
 *
 * @returns 0 on success, negative error code upon failure
 */
int set_flags(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t flags;
  uint64_t mask;
  uint64_t snap_id = CEPH_NOSNAP;
  auto iter = in->cbegin();
  try {
    decode(flags, iter);
    decode(mask, iter);
    if (!iter.end()) {
      decode(snap_id, iter);
    }
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // check that size exists to make sure this is a header object
  // that was created correctly
  int r;
  uint64_t orig_flags = 0;
  cls_rbd_snap snap_meta;
  string snap_meta_key;
  if (snap_id == CEPH_NOSNAP) {
    r = read_key(hctx, "flags", &orig_flags);
    if (r < 0 && r != -ENOENT) {
      CLS_ERR("Could not read image's flags off disk: %s",
              cpp_strerror(r).c_str());
      return r;
    }
  } else {
    key_from_snap_id(snap_id, &snap_meta_key);
    r = read_key(hctx, snap_meta_key, &snap_meta);
    if (r < 0) {
      CLS_ERR("Could not read snapshot: snap_id=%" PRIu64 ": %s",
              snap_id, cpp_strerror(r).c_str());
      return r;
    }
    orig_flags = snap_meta.flags;
  }

  flags = (orig_flags & ~mask) | (flags & mask);
  CLS_LOG(20, "set_flags snap_id=%" PRIu64 ", orig_flags=%" PRIu64 ", "
              "new_flags=%" PRIu64 ", mask=%" PRIu64, snap_id, orig_flags,
              flags, mask);

  if (snap_id == CEPH_NOSNAP) {
    r = write_key(hctx, "flags", flags);
  } else {
    snap_meta.flags = flags;
    r = image::snapshot::write(hctx, snap_meta_key, std::move(snap_meta));
  }

  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Get the operation-based image features
 *
 * Input:
 *
 * Output:
 * @param bitmask of enabled op features (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int op_features_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "op_features_get");

  uint64_t op_features = 0;
  int r = read_key(hctx, "op_features", &op_features);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("failed to read op features off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  encode(op_features, *out);
  return 0;
}

/**
 * Set the operation-based image features
 *
 * Input:
 * @param op_features image op features
 * @param mask image op feature mask
 *
 * Output:
 * none
 *
 * @returns 0 on success, negative error code upon failure
 */
int op_features_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t op_features;
  uint64_t mask;
  auto iter = in->cbegin();
  try {
    decode(op_features, iter);
    decode(mask, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  uint64_t unsupported_op_features = (mask & ~RBD_OPERATION_FEATURES_ALL);
  if (unsupported_op_features != 0ULL) {
    CLS_ERR("unsupported op features: %" PRIu64, unsupported_op_features);
    return -EINVAL;
  }

  return image::set_op_features(hctx, op_features, mask);
}

/**
 * get the current parent, if any
 *
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param pool parent pool id (-1 if parent does not exist)
 * @param image parent image id
 * @param snapid parent snapid
 * @param size portion of parent mapped under the child
 *
 * @returns 0 on success or parent does not exist, negative error code on failure
 */
int get_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0) {
    return r;
  }

  CLS_LOG(20, "get_parent snap_id=%" PRIu64, snap_id);

  cls_rbd_parent parent;
  r = image::require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r == 0) {
    r = read_key(hctx, "parent", &parent);
    if (r < 0 && r != -ENOENT) {
      return r;
    } else if (!parent.pool_namespace.empty()) {
      return -EXDEV;
    }

    if (snap_id != CEPH_NOSNAP) {
      cls_rbd_snap snap;
      std::string snapshot_key;
      key_from_snap_id(snap_id, &snapshot_key);
      r = read_key(hctx, snapshot_key, &snap);
      if (r < 0 && r != -ENOENT) {
	return r;
      }

      if (snap.parent.exists()) {
        // legacy format where full parent spec is written within
        // each snapshot record
        parent = snap.parent;
      } else if (snap.parent_overlap) {
        // normalized parent reference
        if (!parent.exists()) {
          CLS_ERR("get_parent: snap_id=%" PRIu64 ": invalid parent spec",
                  snap_id);
          return -EINVAL;
        }
        parent.head_overlap = *snap.parent_overlap;
      } else {
        // snapshot doesn't have associated parent
        parent = {};
      }
    }
  }

  encode(parent.pool_id, *out);
  encode(parent.image_id, *out);
  encode(parent.snap_id, *out);
  encode(parent.head_overlap.value_or(0ULL), *out);
  return 0;
}

/**
 * set the image parent
 *
 * Input:
 * @param pool parent pool
 * @param id parent image id
 * @param snapid parent snapid
 * @param size parent size
 *
 * @returns 0 on success, or negative error code
 */
int set_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_rbd_parent parent;
  auto iter = in->cbegin();
  try {
    decode(parent.pool_id, iter);
    decode(parent.image_id, iter);
    decode(parent.snap_id, iter);

    uint64_t overlap;
    decode(overlap, iter);
    parent.head_overlap = overlap;
  } catch (const buffer::error &err) {
    CLS_LOG(20, "cls_rbd::set_parent: invalid decode");
    return -EINVAL;
  }

  int r = image::parent::attach(hctx, parent, false);
  if (r < 0) {
    return r;
  }

  return 0;
}


/**
 * remove the parent pointer
 *
 * This can only happen on the head, not on a snapshot.  No arguments.
 *
 * @returns 0 on success, negative error code on failure.
 */
int remove_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = image::parent::detach(hctx, true);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param parent spec (cls::rbd::ParentImageSpec)
 * @returns 0 on success, negative error code on failure
 */
int parent_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r = check_exists(hctx);
  if (r < 0) {
    return r;
  }

  CLS_LOG(20, "parent_get");

  cls_rbd_parent parent;
  r = image::require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r == 0) {
    r = read_key(hctx, "parent", &parent);
    if (r < 0 && r != -ENOENT) {
      return r;
    } else if (r == -ENOENT) {
      // examine oldest snapshot to see if it has a denormalized parent
      auto parent_lambda = [&parent](const cls_rbd_snap& snap_meta) {
        if (snap_meta.parent.exists()) {
          parent = snap_meta.parent;
        }
        return 0;
      };

      r = image::snapshot::iterate(hctx, parent_lambda);
      if (r < 0) {
        return r;
      }
    }
  }

  cls::rbd::ParentImageSpec parent_image_spec{
    parent.pool_id, parent.pool_namespace, parent.image_id,
    parent.snap_id};
  encode(parent_image_spec, *out);
  return 0;
}

/**
 * Input:
 * @param snap id (uint64_t) parent snapshot id
 *
 * Output:
 * @param byte overlap of parent image (std::optional<uint64_t>)
 * @returns 0 on success, negative error code on failure
 */
int parent_overlap_get(cls_method_context_t hctx, bufferlist *in,
                       bufferlist *out) {
  uint64_t snap_id;
  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = check_exists(hctx);
  CLS_LOG(20, "parent_overlap_get");

  std::optional<uint64_t> parent_overlap = std::nullopt;
  r = image::require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r == 0) {
    if (snap_id == CEPH_NOSNAP) {
      cls_rbd_parent parent;
      r = read_key(hctx, "parent", &parent);
      if (r < 0 && r != -ENOENT) {
        return r;
      } else if (r == 0) {
        parent_overlap = parent.head_overlap;
      }
    } else {
      cls_rbd_snap snap;
      std::string snapshot_key;
      key_from_snap_id(snap_id, &snapshot_key);
      r = read_key(hctx, snapshot_key, &snap);
      if (r < 0) {
        return r;
      }

      if (snap.parent_overlap) {
        parent_overlap = snap.parent_overlap;
      } else if (snap.parent.exists()) {
        // legacy format where full parent spec is written within
        // each snapshot record
        parent_overlap = snap.parent.head_overlap;
      }
    }
  };

  encode(parent_overlap, *out);
  return 0;
}

/**
 * Input:
 * @param parent spec (cls::rbd::ParentImageSpec)
 * @param size parent size (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int parent_attach(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls::rbd::ParentImageSpec parent_image_spec;
  uint64_t parent_overlap;
  bool reattach = false;

  auto iter = in->cbegin();
  try {
    decode(parent_image_spec, iter);
    decode(parent_overlap, iter);
    if (!iter.end()) {
      decode(reattach, iter);
    }
  } catch (const buffer::error &err) {
    CLS_LOG(20, "cls_rbd::parent_attach: invalid decode");
    return -EINVAL;
  }

  int r = image::parent::attach(hctx, {parent_image_spec, parent_overlap},
                                reattach);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int parent_detach(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  int r = image::parent::detach(hctx, false);
  if (r < 0) {
    return r;
  }

  return 0;
}


/**
 * methods for dealing with rbd_children object
 */

static int decode_parent_common(bufferlist::const_iterator& it, uint64_t *pool_id,
				string *image_id, snapid_t *snap_id)
{
  try {
    decode(*pool_id, it);
    decode(*image_id, it);
    decode(*snap_id, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding parent spec");
    return -EINVAL;
  }
  return 0;
}

static int decode_parent(bufferlist *in, uint64_t *pool_id,
			 string *image_id, snapid_t *snap_id)
{
  auto it = in->cbegin();
  return decode_parent_common(it, pool_id, image_id, snap_id);
}

static int decode_parent_and_child(bufferlist *in, uint64_t *pool_id,
			           string *image_id, snapid_t *snap_id,
				   string *c_image_id)
{
  auto it = in->cbegin();
  int r = decode_parent_common(it, pool_id, image_id, snap_id);
  if (r < 0)
    return r;
  try {
    decode(*c_image_id, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding child image id");
    return -EINVAL;
  }
  return 0;
}

static string parent_key(uint64_t pool_id, string image_id, snapid_t snap_id)
{
  bufferlist key_bl;
  encode(pool_id, key_bl);
  encode(image_id, key_bl);
  encode(snap_id, key_bl);
  return string(key_bl.c_str(), key_bl.length());
}

/**
 * add child to rbd_children directory object
 *
 * rbd_children is a map of (p_pool_id, p_image_id, p_snap_id) to
 * [c_image_id, [c_image_id ... ]]
 *
 * Input:
 * @param p_pool_id parent pool id
 * @param p_image_id parent image oid
 * @param p_snap_id parent snapshot id
 * @param c_image_id new child image oid to add
 *
 * @returns 0 on success, negative error on failure
 */

int add_child(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r;

  uint64_t p_pool_id;
  snapid_t p_snap_id;
  string p_image_id, c_image_id;
  // Use set for ease of erase() for remove_child()
  std::set<string> children;

  r = decode_parent_and_child(in, &p_pool_id, &p_image_id, &p_snap_id,
			      &c_image_id);
  if (r < 0)
    return r;

  CLS_LOG(20, "add_child %s to (%" PRIu64 ", %s, %" PRIu64 ")", c_image_id.c_str(),
	  p_pool_id, p_image_id.c_str(), p_snap_id.val);

  string key = parent_key(p_pool_id, p_image_id, p_snap_id);

  // get current child list for parent, if any
  r = read_key(hctx, key, &children);
  if ((r < 0) && (r != -ENOENT)) {
    CLS_LOG(20, "add_child: omap read failed: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (children.find(c_image_id) != children.end()) {
    CLS_LOG(20, "add_child: child already exists: %s", c_image_id.c_str());
    return -EEXIST;
  }
  // add new child
  children.insert(c_image_id);

  // write back
  bufferlist childbl;
  encode(children, childbl);
  r = cls_cxx_map_set_val(hctx, key, &childbl);
  if (r < 0)
    CLS_LOG(20, "add_child: omap write failed: %s", cpp_strerror(r).c_str());
  return r;
}

/**
 * remove child from rbd_children directory object
 *
 * Input:
 * @param p_pool_id parent pool id
 * @param p_image_id parent image oid
 * @param p_snap_id parent snapshot id
 * @param c_image_id new child image oid to add
 *
 * @returns 0 on success, negative error on failure
 */

int remove_child(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r;

  uint64_t p_pool_id;
  snapid_t p_snap_id;
  string p_image_id, c_image_id;
  std::set<string> children;

  r = decode_parent_and_child(in, &p_pool_id, &p_image_id, &p_snap_id,
			      &c_image_id);
  if (r < 0)
    return r;

  CLS_LOG(20, "remove_child %s from (%" PRIu64 ", %s, %" PRIu64 ")",
	       c_image_id.c_str(), p_pool_id, p_image_id.c_str(),
	       p_snap_id.val);

  string key = parent_key(p_pool_id, p_image_id, p_snap_id);

  // get current child list for parent.  Unlike add_child(), an empty list
  // is an error (how can we remove something that doesn't exist?)
  r = read_key(hctx, key, &children);
  if (r < 0) {
    CLS_LOG(20, "remove_child: read omap failed: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (children.find(c_image_id) == children.end()) {
    CLS_LOG(20, "remove_child: child not found: %s", c_image_id.c_str());
    return -ENOENT;
  }
  // find and remove child
  children.erase(c_image_id);

  // now empty?  remove key altogether
  if (children.empty()) {
    r = cls_cxx_map_remove_key(hctx, key);
    if (r < 0)
      CLS_LOG(20, "remove_child: remove key failed: %s", cpp_strerror(r).c_str());
  } else {
    // write back shortened children list
    bufferlist childbl;
    encode(children, childbl);
    r = cls_cxx_map_set_val(hctx, key, &childbl);
    if (r < 0)
      CLS_LOG(20, "remove_child: write omap failed: %s", cpp_strerror(r).c_str());
  }
  return r;
}

/**
 * Input:
 * @param p_pool_id parent pool id
 * @param p_image_id parent image oid
 * @param p_snap_id parent snapshot id
 * @param c_image_id new child image oid to add
 *
 * Output:
 * @param children set<string> of children
 *
 * @returns 0 on success, negative error on failure
 */
int get_children(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r;
  uint64_t p_pool_id;
  snapid_t p_snap_id;
  string p_image_id;
  std::set<string> children;

  r = decode_parent(in, &p_pool_id, &p_image_id, &p_snap_id);
  if (r < 0)
    return r;

  CLS_LOG(20, "get_children of (%" PRIu64 ", %s, %" PRIu64 ")",
	  p_pool_id, p_image_id.c_str(), p_snap_id.val);

  string key = parent_key(p_pool_id, p_image_id, p_snap_id);

  r = read_key(hctx, key, &children);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_LOG(20, "get_children: read omap failed: %s", cpp_strerror(r).c_str());
    return r;
  }
  encode(children, *out);
  return 0;
}


/**
 * Get the information needed to create a rados snap context for doing
 * I/O to the data objects. This must include all snapshots.
 *
 * Output:
 * @param snap_seq the highest snapshot id ever associated with the image (uint64_t)
 * @param snap_ids existing snapshot ids in descending order (vector<uint64_t>)
 * @returns 0 on success, negative error code on failure
 */
int get_snapcontext(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_snapcontext");

  int r;
  int max_read = RBD_MAX_KEYS_READ;
  vector<snapid_t> snap_ids;
  string last_read = RBD_SNAP_KEY_PREFIX;
  bool more;

  do {
    set<string> keys;
    r = cls_cxx_map_get_keys(hctx, last_read, max_read, &keys, &more);
    if (r < 0)
      return r;

    for (set<string>::const_iterator it = keys.begin();
	 it != keys.end(); ++it) {
      if ((*it).find(RBD_SNAP_KEY_PREFIX) != 0)
	break;
      snapid_t snap_id = snap_id_from_key(*it);
      snap_ids.push_back(snap_id);
    }
    if (!keys.empty())
      last_read = *(keys.rbegin());
  } while (more);

  uint64_t snap_seq;
  r = read_key(hctx, "snap_seq", &snap_seq);
  if (r < 0) {
    CLS_ERR("could not read the image's snap_seq off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  // snap_ids must be descending in a snap context
  std::reverse(snap_ids.begin(), snap_ids.end());

  encode(snap_seq, *out);
  encode(snap_ids, *out);

  return 0;
}

/**
 * Output:
 * @param object_prefix prefix for data object names (string)
 * @returns 0 on success, negative error code on failure
 */
int get_object_prefix(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_object_prefix");

  string object_prefix;
  int r = read_key(hctx, "object_prefix", &object_prefix);
  if (r < 0) {
    CLS_ERR("failed to read the image's object prefix off of disk: %s",
            cpp_strerror(r).c_str());
    return r;
  }

  encode(object_prefix, *out);

  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param pool_id (int64_t) of data pool or -1 if none
 * @returns 0 on success, negative error code on failure
 */
int get_data_pool(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_data_pool");

  int64_t data_pool_id = -1;
  int r = read_key(hctx, "data_pool_id", &data_pool_id);
  if (r == -ENOENT) {
    data_pool_id = -1;
  } else if (r < 0) {
    CLS_ERR("error reading image data pool id: %s", cpp_strerror(r).c_str());
    return r;
  }

  encode(data_pool_id, *out);
  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query
 *
 * Output:
 * @param name (string) of the snapshot
 * @returns 0 on success, negative error code on failure
 */
int get_snapshot_name(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_snapshot_name snap_id=%llu", (unsigned long long)snap_id);

  if (snap_id == CEPH_NOSNAP)
    return -EINVAL;

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0)
    return r;

  encode(snap.name, *out);

  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query
 *
 * Output:
 * @param timestamp (utime_t) of the snapshot
 * @returns 0 on success, negative error code on failure
 *
 * NOTE: deprecated - remove this method after Luminous is unsupported
 */
int get_snapshot_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_snapshot_timestamp snap_id=%llu", (unsigned long long)snap_id);

  if (snap_id == CEPH_NOSNAP) {
    return -EINVAL;
  }

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    return r;
  }

  encode(snap.timestamp, *out);
  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query
 *
 * Output:
 * @param snapshot (cls::rbd::SnapshotInfo)
 * @returns 0 on success, negative error code on failure
 */
int snapshot_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  auto iter = in->cbegin();
  try {
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_get snap_id=%llu", (unsigned long long)snap_id);
  if (snap_id == CEPH_NOSNAP) {
    return -EINVAL;
  }

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    return r;
  }

  cls::rbd::SnapshotInfo snapshot_info{snap.id, snap.snapshot_namespace,
                                       snap.name, snap.image_size,
                                       snap.timestamp, snap.child_count};
  encode(snapshot_info, *out);
  return 0;
}

/**
 * Adds a snapshot to an rbd header. Ensures the id and name are unique.
 *
 * Input:
 * @param snap_name name of the snapshot (string)
 * @param snap_id id of the snapshot (uint64_t)
 * @param snap_namespace namespace of the snapshot (cls::rbd::SnapshotNamespace)
 *
 * Output:
 * @returns 0 on success, negative error code on failure.
 * @returns -ESTALE if the input snap_id is less than the image's snap_seq
 * @returns -EEXIST if the id or name are already used by another snapshot
 */
int snapshot_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist snap_namebl, snap_idbl;
  cls_rbd_snap snap_meta;
  uint64_t snap_limit;

  try {
    auto iter = in->cbegin();
    decode(snap_meta.name, iter);
    decode(snap_meta.id, iter);
    if (!iter.end()) {
      decode(snap_meta.snapshot_namespace, iter);
    }
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (boost::get<cls::rbd::UnknownSnapshotNamespace>(
        &snap_meta.snapshot_namespace) != nullptr) {
    CLS_ERR("Unknown snapshot namespace provided");
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_add name=%s id=%llu", snap_meta.name.c_str(),
	 (unsigned long long)snap_meta.id.val);

  if (snap_meta.id > CEPH_MAXSNAP)
    return -EINVAL;

  uint64_t cur_snap_seq;
  int r = read_key(hctx, "snap_seq", &cur_snap_seq);
  if (r < 0) {
    CLS_ERR("Could not read image's snap_seq off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  // client lost a race with another snapshot creation.
  // snap_seq must be monotonically increasing.
  if (snap_meta.id < cur_snap_seq)
    return -ESTALE;

  r = read_key(hctx, "size", &snap_meta.image_size);
  if (r < 0) {
    CLS_ERR("Could not read image's size off disk: %s", cpp_strerror(r).c_str());
    return r;
  }
  r = read_key(hctx, "flags", &snap_meta.flags);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("Could not read image's flags off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  r = read_key(hctx, "snap_limit", &snap_limit);
  if (r == -ENOENT) {
    snap_limit = UINT64_MAX;
  } else if (r < 0) {
    CLS_ERR("Could not read snapshot limit off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  snap_meta.timestamp = ceph_clock_now();

  uint64_t total_read = 0;
  auto pre_check_lambda =
    [&snap_meta, &total_read, snap_limit](const cls_rbd_snap& old_meta) {
      ++total_read;
      if (total_read >= snap_limit) {
        CLS_ERR("Attempt to create snapshot over limit of %" PRIu64,
                snap_limit);
        return -EDQUOT;
      }

      if ((snap_meta.name == old_meta.name &&
	    snap_meta.snapshot_namespace == old_meta.snapshot_namespace) ||
	  snap_meta.id == old_meta.id) {
	CLS_LOG(20, "snap_name %s or snap_id %" PRIu64 " matches existing snap "
                "%s %" PRIu64, snap_meta.name.c_str(), snap_meta.id.val,
		old_meta.name.c_str(), old_meta.id.val);
	return -EEXIST;
      }
      return 0;
    };

  r = image::snapshot::iterate(hctx, pre_check_lambda);
  if (r < 0) {
    return r;
  }

  // snapshot inherits parent, if any
  cls_rbd_parent parent;
  r = read_key(hctx, "parent", &parent);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  if (r == 0) {
    // write helper method will convert to normalized format if required
    snap_meta.parent = parent;
  }

  if (cls::rbd::get_snap_namespace_type(snap_meta.snapshot_namespace) ==
        cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
    // add snap_trash feature bit if not already enabled
    r = image::set_op_features(hctx, RBD_OPERATION_FEATURE_SNAP_TRASH,
                               RBD_OPERATION_FEATURE_SNAP_TRASH);
    if (r < 0) {
      return r;
    }
  }

  r = write_key(hctx, "snap_seq", snap_meta.id);
  if (r < 0) {
    return r;
  }

  std::string snapshot_key;
  key_from_snap_id(snap_meta.id, &snapshot_key);
  r = image::snapshot::write(hctx, snapshot_key, std::move(snap_meta));
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * rename snapshot .
 *
 * Input:
 * @param src_snap_id old snap id of the snapshot (snapid_t)
 * @param dst_snap_name new name of the snapshot (string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure.
 */
int snapshot_rename(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist snap_namebl, snap_idbl;
  snapid_t src_snap_id;
  string dst_snap_name;
  cls_rbd_snap snap_meta;
  int r;

  try {
    auto iter = in->cbegin();
    decode(src_snap_id, iter);
    decode(dst_snap_name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_rename id=%" PRIu64 ", dst_name=%s",
          src_snap_id.val, dst_snap_name.c_str());

  auto duplicate_name_lambda = [&dst_snap_name](const cls_rbd_snap& snap_meta) {
    if (cls::rbd::get_snap_namespace_type(snap_meta.snapshot_namespace) ==
          cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER &&
        snap_meta.name == dst_snap_name) {
      CLS_LOG(20, "snap_name %s matches existing snap with snap id %" PRIu64,
              dst_snap_name.c_str(), snap_meta.id.val);
      return -EEXIST;
    }
    return 0;
  };
  r = image::snapshot::iterate(hctx, duplicate_name_lambda);
  if (r < 0) {
    return r;
  }

  std::string src_snap_key;
  key_from_snap_id(src_snap_id, &src_snap_key);
  r = read_key(hctx, src_snap_key, &snap_meta);
  if (r == -ENOENT) {
    CLS_LOG(20, "cannot find existing snap with snap id = %" PRIu64,
            src_snap_id.val);
    return r;
  }

  if (cls::rbd::get_snap_namespace_type(snap_meta.snapshot_namespace) !=
        cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER) {
    // can only rename user snapshots
    return -EINVAL;
  }

  snap_meta.name = dst_snap_name;
  r = image::snapshot::write(hctx, src_snap_key, std::move(snap_meta));
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Removes a snapshot from an rbd header.
 *
 * Input:
 * @param snap_id the id of the snapshot to remove (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int snapshot_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  snapid_t snap_id;

  try {
    auto iter = in->cbegin();
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_remove id=%llu", (unsigned long long)snap_id.val);

  // check if the key exists. we can't rely on remove_key doing this for
  // us, since OMAPRMKEYS returns success if the key is not there.
  // bug or feature? sounds like a bug, since tmap did not have this
  // behavior, but cls_rgw may rely on it...
  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r == -ENOENT) {
    return -ENOENT;
  }

  if (snap.protection_status != RBD_PROTECTION_STATUS_UNPROTECTED) {
    return -EBUSY;
  }

  // snapshot is in-use by clone v2 child
  if (snap.child_count > 0) {
    return -EBUSY;
  }

  r = remove_key(hctx, snapshot_key);
  if (r < 0) {
    return r;
  }

  bool has_child_snaps = false;
  bool has_trash_snaps = false;
  auto remove_lambda = [snap_id, &has_child_snaps, &has_trash_snaps](
      const cls_rbd_snap& snap_meta) {
    if (snap_meta.id != snap_id) {
      if (snap_meta.parent.pool_id != -1 || snap_meta.parent_overlap) {
        has_child_snaps = true;
      }

      if (cls::rbd::get_snap_namespace_type(snap_meta.snapshot_namespace) ==
            cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
        has_trash_snaps = true;
      }
    }
    return 0;
  };

  r = image::snapshot::iterate(hctx, remove_lambda);
  if (r < 0) {
    return r;
  }

  cls_rbd_parent parent;
  r = read_key(hctx, "parent", &parent);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  bool has_parent = (r >= 0 && parent.exists());
  bool is_head_child = (has_parent && parent.head_overlap);
  int8_t require_osd_release = cls_get_required_osd_release(hctx);
  if (has_parent && !is_head_child && !has_child_snaps &&
      require_osd_release >= CEPH_RELEASE_NAUTILUS) {
    // remove the unused parent image spec
    r = remove_key(hctx, "parent");
    if (r < 0 && r != -ENOENT) {
      return r;
    }
  }

  uint64_t op_features_mask = 0ULL;
  if (!has_child_snaps && !is_head_child) {
    // disable clone child op feature if no longer associated
    op_features_mask |= RBD_OPERATION_FEATURE_CLONE_CHILD;
  }
  if (!has_trash_snaps) {
    // remove the snap_trash op feature if not in-use by any other snapshots
    op_features_mask |= RBD_OPERATION_FEATURE_SNAP_TRASH;
  }

  if (op_features_mask != 0ULL) {
    r = image::set_op_features(hctx, 0, op_features_mask);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

/**
 * Moves a snapshot to the trash namespace.
 *
 * Input:
 * @param snap_id the id of the snapshot to move to the trash (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int snapshot_trash_add(cls_method_context_t hctx, bufferlist *in,
                       bufferlist *out)
{
  snapid_t snap_id;

  try {
    auto iter = in->cbegin();
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_trash_add id=%" PRIu64, snap_id.val);

  cls_rbd_snap snap;
  std::string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r == -ENOENT) {
    return r;
  }

  if (snap.protection_status != RBD_PROTECTION_STATUS_UNPROTECTED) {
    return -EBUSY;
  }

  auto snap_type = cls::rbd::get_snap_namespace_type(snap.snapshot_namespace);
  if (snap_type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
    return -EEXIST;
  }

  // add snap_trash feature bit if not already enabled
  r = image::set_op_features(hctx, RBD_OPERATION_FEATURE_SNAP_TRASH,
                             RBD_OPERATION_FEATURE_SNAP_TRASH);
  if (r < 0) {
    return r;
  }

  snap.snapshot_namespace = cls::rbd::TrashSnapshotNamespace{snap_type,
                                                             snap.name};
  uuid_d uuid_gen;
  uuid_gen.generate_random();
  snap.name = uuid_gen.to_string();

  r = image::snapshot::write(hctx, snapshot_key, std::move(snap));
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Returns a uint64_t of all the features supported by this class.
 */
int get_all_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t all_features = RBD_FEATURES_ALL;
  encode(all_features, *out);
  return 0;
}

/**
 * "Copy up" data from the parent of a clone to the clone's object(s).
 * Used for implementing copy-on-write for a clone image.  Client
 * will pass down a chunk of data that fits completely within one
 * clone block (one object), and is aligned (starts at beginning of block),
 * but may be shorter (for non-full parent blocks).  The class method
 * can't know the object size to validate the requested length,
 * so it just writes the data as given if the child object doesn't
 * already exist, and returns success if it does.
 *
 * Input:
 * @param in bufferlist of data to write
 *
 * Output:
 * @returns 0 on success, or if block already exists in child
 *  negative error code on other error
 */

int copyup(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // check for existence; if child object exists, just return success
  if (cls_cxx_stat(hctx, NULL, NULL) == 0)
    return 0;
  CLS_LOG(20, "copyup: writing length %d\n", in->length());
  return cls_cxx_write(hctx, 0, in->length(), in);
}


/************************ rbd_id object methods **************************/

/**
 * Input:
 * @param in ignored
 *
 * Output:
 * @param id the id stored in the object
 * @returns 0 on success, negative error code on failure
 */
int get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;

  if (size == 0)
    return -ENOENT;

  bufferlist read_bl;
  r = cls_cxx_read(hctx, 0, size, &read_bl);
  if (r < 0) {
    CLS_ERR("get_id: could not read id: %s", cpp_strerror(r).c_str());
    return r;
  }

  string id;
  try {
    auto iter = read_bl.cbegin();
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EIO;
  }

  encode(id, *out);
  return 0;
}

/**
 * Set the id of an image. The object must already exist.
 *
 * Input:
 * @param id the id of the image, as an alpha-numeric string
 *
 * Output:
 * @returns 0 on success, -EEXIST if the atomic create fails,
 *          negative error code on other error
 */
int set_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  string id;
  try {
    auto iter = in->cbegin();
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (!is_valid_id(id)) {
    CLS_ERR("set_id: invalid id '%s'", id.c_str());
    return -EINVAL;
  }

  uint64_t size;
  r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;
  if (size != 0)
    return -EEXIST;

  CLS_LOG(20, "set_id: id=%s", id.c_str());

  bufferlist write_bl;
  encode(id, write_bl);
  return cls_cxx_write(hctx, 0, write_bl.length(), &write_bl);
}

/**
 * Update the access timestamp of an image
 *
 * Input:
 * @param none
 *
 * Output:
 * @returns 0 on success, negative error code on other error
 */
int set_access_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
   int r = check_exists(hctx);
   if(r < 0)
     return r;
   
   utime_t timestamp = ceph_clock_now();
   r = write_key(hctx, "access_timestamp", timestamp);
   if(r < 0) {
     CLS_ERR("error setting access_timestamp");
     return r;
   }

   return 0;
}

/**
 * Update the modify timestamp of an image
 *
 * Input:
 * @param none
 *
 * Output:
 * @returns 0 on success, negative error code on other error
 */

int set_modify_timestamp(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
   int r = check_exists(hctx);
   if(r < 0)
     return r;
   
   utime_t timestamp = ceph_clock_now();
   r = write_key(hctx, "modify_timestamp", timestamp);
   if(r < 0) {
     CLS_ERR("error setting modify_timestamp");
     return r;
   }

   return 0;
}



/*********************** methods for rbd_directory ***********************/

static const string dir_key_for_id(const string &id)
{
  return RBD_DIR_ID_KEY_PREFIX + id;
}

static const string dir_key_for_name(const string &name)
{
  return RBD_DIR_NAME_KEY_PREFIX + name;
}

static const string dir_name_from_key(const string &key)
{
  return key.substr(strlen(RBD_DIR_NAME_KEY_PREFIX));
}

static int dir_add_image_helper(cls_method_context_t hctx,
				const string &name, const string &id,
				bool check_for_unique_id)
{
  if (!name.size() || !is_valid_id(id)) {
    CLS_ERR("dir_add_image_helper: invalid name '%s' or id '%s'",
	    name.c_str(), id.c_str());
    return -EINVAL;
  }

  CLS_LOG(20, "dir_add_image_helper name=%s id=%s", name.c_str(), id.c_str());

  string tmp;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &tmp);
  if (r != -ENOENT) {
    CLS_LOG(10, "name already exists");
    return -EEXIST;
  }
  r = read_key(hctx, id_key, &tmp);
  if (r != -ENOENT && check_for_unique_id) {
    CLS_LOG(10, "id already exists");
    return -EBADF;
  }
  bufferlist id_bl, name_bl;
  encode(id, id_bl);
  encode(name, name_bl);
  map<string, bufferlist> omap_vals;
  omap_vals[name_key] = id_bl;
  omap_vals[id_key] = name_bl;
  return cls_cxx_map_set_vals(hctx, &omap_vals);
}

static int dir_remove_image_helper(cls_method_context_t hctx,
				   const string &name, const string &id)
{
  CLS_LOG(20, "dir_remove_image_helper name=%s id=%s",
	  name.c_str(), id.c_str());

  string stored_name, stored_id;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &stored_id);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading name to id mapping: %s", cpp_strerror(r).c_str());
    return r;
  }
  r = read_key(hctx, id_key, &stored_name);
  if (r < 0) {
    CLS_ERR("error reading id to name mapping: %s", cpp_strerror(r).c_str());
    return r;
  }

  // check if this op raced with a rename
  if (stored_name != name || stored_id != id) {
    CLS_ERR("stored name '%s' and id '%s' do not match args '%s' and '%s'",
	    stored_name.c_str(), stored_id.c_str(), name.c_str(), id.c_str());
    return -ESTALE;
  }

  r = cls_cxx_map_remove_key(hctx, name_key);
  if (r < 0) {
    CLS_ERR("error removing name: %s", cpp_strerror(r).c_str());
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, id_key);
  if (r < 0) {
    CLS_ERR("error removing id: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/**
 * Rename an image in the directory, updating both indexes
 * atomically. This can't be done from the client calling
 * dir_add_image and dir_remove_image in one transaction because the
 * results of the first method are not visibale to later steps.
 *
 * Input:
 * @param src original name of the image
 * @param dest new name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if src and id do not map to each other
 * @returns -ENOENT if src or id are not in the directory
 * @returns -EEXIST if dest already exists
 * @returns 0 on success, negative error code on failure
 */
int dir_rename_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string src, dest, id;
  try {
    auto iter = in->cbegin();
    decode(src, iter);
    decode(dest, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = dir_remove_image_helper(hctx, src, id);
  if (r < 0)
    return r;
  // ignore duplicate id because the result of
  // remove_image_helper is not visible yet
  return dir_add_image_helper(hctx, dest, id, false);
}

/**
 * Get the id of an image given its name.
 *
 * Input:
 * @param name the name of the image
 *
 * Output:
 * @param id the id of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name;

  try {
    auto iter = in->cbegin();
    decode(name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_id: name=%s", name.c_str());

  string id;
  int r = read_key(hctx, dir_key_for_name(name), &id);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading id for name '%s': %s", name.c_str(), cpp_strerror(r).c_str());
    return r;
  }
  encode(id, *out);
  return 0;
}

/**
 * Get the name of an image given its id.
 *
 * Input:
 * @param id the id of the image
 *
 * Output:
 * @param name the name of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_name(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;

  try {
    auto iter = in->cbegin();
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_name: id=%s", id.c_str());

  string name;
  int r = read_key(hctx, dir_key_for_id(id), &name);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading name for id '%s': %s", id.c_str(),
              cpp_strerror(r).c_str());
    }
    return r;
  }
  encode(name, *out);
  return 0;
}

/**
 * List the names and ids of the images in the directory, sorted by
 * name.
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param images map from name to id of up to max_return images
 * @returns 0 on success, negative error code on failure
 */
int dir_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  map<string, string> images;
  string last_read = dir_key_for_name(start_after);
  bool more = true;

  while (more && images.size() < max_return) {
    map<string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    int r = cls_cxx_map_get_vals(hctx, last_read, RBD_DIR_NAME_KEY_PREFIX,
                                 max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading directory by name: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end(); ++it) {
      string id;
      auto iter = it->second.cbegin();
      try {
	decode(id, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode id of image '%s'", it->first.c_str());
	return -EIO;
      }
      CLS_LOG(20, "adding '%s' -> '%s'", dir_name_from_key(it->first).c_str(), id.c_str());
      images[dir_name_from_key(it->first)] = id;
      if (images.size() >= max_return)
	break;
    }
    if (!vals.empty()) {
      last_read = dir_key_for_name(images.rbegin()->first);
    }
  }

  encode(images, *out);

  return 0;
}

/**
 * Add an image to the rbd directory. Creates the directory object if
 * needed, and updates the index from id to name and name to id.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -EEXIST if the image name is already in the directory
 * @returns -EBADF if the image id is already in the directory
 * @returns 0 on success, negative error code on failure
 */
int dir_add_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_cxx_create(hctx, false);
  if (r < 0) {
    CLS_ERR("could not create directory: %s", cpp_strerror(r).c_str());
    return r;
  }

  string name, id;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_add_image_helper(hctx, name, id, true);
}

/**
 * Remove an image from the rbd directory.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if the name and id do not map to each other
 * @returns 0 on success, negative error code on failure
 */
int dir_remove_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name, id;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_remove_image_helper(hctx, name, id);
}

/**
 * Verify the current state of the directory
 *
 * Input:
 * @param state the DirectoryState of the directory
 *
 * Output:
 * @returns -ENOENT if the state does not match
 * @returns 0 on success, negative error code on failure
 */
int dir_state_assert(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls::rbd::DirectoryState directory_state = cls::rbd::DIRECTORY_STATE_READY;
  try {
    auto iter = in->cbegin();
    decode(directory_state, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  cls::rbd::DirectoryState on_disk_directory_state = directory_state;
  int r = read_key(hctx, "state", &on_disk_directory_state);
  if (r < 0) {
    return r;
  }

  if (directory_state != on_disk_directory_state) {
    return -ENOENT;
  }
  return 0;
}

/**
 * Set the current state of the directory
 *
 * Input:
 * @param state the DirectoryState of the directory
 *
 * Output:
 * @returns -ENOENT if the state does not match
 * @returns 0 on success, negative error code on failure
 */
int dir_state_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls::rbd::DirectoryState directory_state;
  try {
    auto iter = in->cbegin();
    decode(directory_state, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  switch (directory_state) {
  case cls::rbd::DIRECTORY_STATE_READY:
    break;
  case cls::rbd::DIRECTORY_STATE_ADD_DISABLED:
    {
      if (r == -ENOENT) {
        return r;
      }

      // verify that the directory is empty
      std::map<std::string, bufferlist> vals;
      bool more;
      r = cls_cxx_map_get_vals(hctx, RBD_DIR_NAME_KEY_PREFIX,
                               RBD_DIR_NAME_KEY_PREFIX, 1, &vals, &more);
      if (r < 0) {
        return r;
      } else if (!vals.empty()) {
        return -EBUSY;
      }
    }
    break;
  default:
    return -EINVAL;
  }

  r = write_key(hctx, "state", directory_state);
  if (r < 0) {
    return r;
  }

  return 0;
}

int object_map_read(cls_method_context_t hctx, BitVector<2> &object_map)
{
  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0) {
    return r;
  }
  if (size == 0) {
    return -ENOENT;
  }

  bufferlist bl;
  r = cls_cxx_read(hctx, 0, size, &bl);
  if (r < 0) {
   return r;
  }

  try {
    auto iter = bl.cbegin();
    decode(object_map, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode object map: %s", err.what());
    return -EINVAL;
  }
  return 0;
}

/**
 * Load an rbd image's object map
 *
 * Input:
 * none
 *
 * Output:
 * @param object map bit vector
 * @returns 0 on success, negative error code on failure
 */
int object_map_load(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  BitVector<2> object_map;
  int r = object_map_read(hctx, object_map);
  if (r < 0) {
    return r;
  }

  object_map.set_crc_enabled(false);
  encode(object_map, *out);
  return 0;
}

/**
 * Save an rbd image's object map
 *
 * Input:
 * @param object map bit vector
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int object_map_save(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  BitVector<2> object_map;
  try {
    auto iter = in->cbegin();
    decode(object_map, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  object_map.set_crc_enabled(true);

  bufferlist bl;
  encode(object_map, bl);
  CLS_LOG(20, "object_map_save: object size=%" PRIu64 ", byte size=%u",
	  object_map.size(), bl.length());
  return cls_cxx_write_full(hctx, &bl);
}

/**
 * Resize an rbd image's object map
 *
 * Input:
 * @param object_count the max number of objects in the image
 * @param default_state the default state of newly created objects
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int object_map_resize(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t object_count;
  uint8_t default_state;
  try {
    auto iter = in->cbegin();
    decode(object_count, iter);
    decode(default_state, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // protect against excessive memory requirements
  if (object_count > cls::rbd::MAX_OBJECT_MAP_OBJECT_COUNT) {
    CLS_ERR("object map too large: %" PRIu64, object_count);
    return -EINVAL;
  }

  BitVector<2> object_map;
  int r = object_map_read(hctx, object_map);
  if ((r < 0) && (r != -ENOENT)) {
    return r;
  }

  size_t orig_object_map_size = object_map.size();
  if (object_count < orig_object_map_size) {
    auto it = object_map.begin() + object_count;
    auto end_it = object_map.end() ;
    uint64_t i = object_count;
    for (; it != end_it; ++it, ++i) {
      if (*it != default_state) {
	CLS_ERR("object map indicates object still exists: %" PRIu64, i);
	return -ESTALE;
      }
    }
    object_map.resize(object_count);
  } else if (object_count > orig_object_map_size) {
    object_map.resize(object_count);
    auto it = object_map.begin() + orig_object_map_size;
    auto end_it = object_map.end();
    for (; it != end_it; ++it) {
      *it = default_state;
    }
  }

  bufferlist map;
  encode(object_map, map);
  CLS_LOG(20, "object_map_resize: object size=%" PRIu64 ", byte size=%u",
	  object_count, map.length());
  return cls_cxx_write_full(hctx, &map);
}

/**
 * Update an rbd image's object map
 *
 * Input:
 * @param start_object_no the start object iterator
 * @param end_object_no the end object iterator
 * @param new_object_state the new object state
 * @param current_object_state optional current object state filter
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int object_map_update(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t start_object_no;
  uint64_t end_object_no;
  uint8_t new_object_state;
  boost::optional<uint8_t> current_object_state;
  try {
    auto iter = in->cbegin();
    decode(start_object_no, iter);
    decode(end_object_no, iter);
    decode(new_object_state, iter);
    decode(current_object_state, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode message");
    return -EINVAL;
  }

  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0) {
    return r;
  }

  BitVector<2> object_map;
  bufferlist header_bl;
  r = cls_cxx_read2(hctx, 0, object_map.get_header_length(), &header_bl,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("object map header read failed");
    return r;
  }

  try {
    auto it = header_bl.cbegin();
    object_map.decode_header(it);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode object map header: %s", err.what());
    return -EINVAL;
  }

  uint64_t object_byte_offset;
  uint64_t byte_length;
  object_map.get_header_crc_extents(&object_byte_offset, &byte_length);

  bufferlist footer_bl;
  r = cls_cxx_read2(hctx, object_byte_offset, byte_length, &footer_bl,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("object map footer read header CRC failed");
    return r;
  }

  try {
    auto it = footer_bl.cbegin();
    object_map.decode_header_crc(it);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode object map header CRC: %s", err.what());
  }

  if (start_object_no >= end_object_no || end_object_no > object_map.size()) {
    return -ERANGE;
  }

  uint64_t object_count = end_object_no - start_object_no;
  object_map.get_data_crcs_extents(start_object_no, object_count,
                                   &object_byte_offset, &byte_length);
  const auto footer_object_offset = object_byte_offset;

  footer_bl.clear();
  r = cls_cxx_read2(hctx, object_byte_offset, byte_length, &footer_bl,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("object map footer read data CRCs failed");
    return r;
  }

  try {
    auto it = footer_bl.cbegin();
    object_map.decode_data_crcs(it, start_object_no);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode object map data CRCs: %s", err.what());
  }

  uint64_t data_byte_offset;
  object_map.get_data_extents(start_object_no, object_count,
                              &data_byte_offset, &object_byte_offset,
                              &byte_length);

  bufferlist data_bl;
  r = cls_cxx_read2(hctx, object_byte_offset, byte_length, &data_bl,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("object map data read failed");
    return r;
  }

  try {
    auto it = data_bl.cbegin();
    object_map.decode_data(it, data_byte_offset);
  } catch (const buffer::error &err) {
    CLS_ERR("failed to decode data chunk [%" PRIu64 "]: %s",
	    data_byte_offset, err.what());
    return -EINVAL;
  }

  bool updated = false;
  auto it = object_map.begin() + start_object_no;
  auto end_it = object_map.begin() + end_object_no;
  for (; it != end_it; ++it) {
    uint8_t state = *it;
    if ((!current_object_state || state == *current_object_state ||
        (*current_object_state == OBJECT_EXISTS &&
         state == OBJECT_EXISTS_CLEAN)) && state != new_object_state) {
      *it = new_object_state;
      updated = true;
    }
  }

  if (updated) {
    CLS_LOG(20, "object_map_update: %" PRIu64 "~%" PRIu64 " -> %" PRIu64,
	    data_byte_offset, byte_length, object_byte_offset);

    bufferlist data_bl;
    object_map.encode_data(data_bl, data_byte_offset, byte_length);
    r = cls_cxx_write2(hctx, object_byte_offset, data_bl.length(), &data_bl,
                       CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("failed to write object map header: %s", cpp_strerror(r).c_str());
      return r;
    }

    footer_bl.clear();
    object_map.encode_data_crcs(footer_bl, start_object_no, object_count);
    r = cls_cxx_write2(hctx, footer_object_offset, footer_bl.length(),
		       &footer_bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("failed to write object map footer: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    CLS_LOG(20, "object_map_update: no update necessary");
  }

  return 0;
}

/**
 * Mark all _EXISTS objects as _EXISTS_CLEAN so future writes to the
 * image HEAD can be tracked.
 *
 * Input:
 * none
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int object_map_snap_add(cls_method_context_t hctx, bufferlist *in,
                        bufferlist *out)
{
  BitVector<2> object_map;
  int r = object_map_read(hctx, object_map);
  if (r < 0) {
    return r;
  }

  bool updated = false;
  auto it = object_map.begin();
  auto end_it = object_map.end();
  for (; it != end_it; ++it) {
    if (*it == OBJECT_EXISTS) {
      *it = OBJECT_EXISTS_CLEAN;
      updated = true;
    }
  }

  if (updated) {
    bufferlist bl;
    encode(object_map, bl);
    r = cls_cxx_write_full(hctx, &bl);
  }
  return r;
}

/**
 * Mark all _EXISTS_CLEAN objects as _EXISTS in the current object map
 * if the provided snapshot object map object is marked as _EXISTS.
 *
 * Input:
 * @param snapshot object map bit vector
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int object_map_snap_remove(cls_method_context_t hctx, bufferlist *in,
                           bufferlist *out)
{
  BitVector<2> src_object_map;
  try {
    auto iter = in->cbegin();
    decode(src_object_map, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  BitVector<2> dst_object_map;
  int r = object_map_read(hctx, dst_object_map);
  if (r < 0) {
    return r;
  }

  bool updated = false;
  auto src_it = src_object_map.begin();
  auto dst_it = dst_object_map.begin();
  auto dst_it_end = dst_object_map.end();
  uint64_t i = 0;
  for (; dst_it != dst_it_end; ++dst_it) {
    if (*dst_it == OBJECT_EXISTS_CLEAN &&
        (i >= src_object_map.size() || *src_it == OBJECT_EXISTS)) {
      *dst_it = OBJECT_EXISTS;
      updated = true;
    }
    if (i < src_object_map.size())
      ++src_it;
    ++i;
  }

  if (updated) {
    bufferlist bl;
    encode(dst_object_map, bl);
    r = cls_cxx_write_full(hctx, &bl);
  }
  return r;
}

static const string metadata_key_for_name(const string &name)
{
  return RBD_METADATA_KEY_PREFIX + name;
}

static const string metadata_name_from_key(const string &key)
{
  return key.substr(strlen(RBD_METADATA_KEY_PREFIX));
}

/**
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list

 * Output:
 * @param value
 * @returns 0 on success, negative error code on failure
 */
int metadata_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // TODO remove implicit support for zero during the N-release
  if (max_return == 0) {
    max_return = RBD_MAX_KEYS_READ;
  }

  map<string, bufferlist> data;
  string last_read = metadata_key_for_name(start_after);
  bool more = true;

  while (more && data.size() < max_return) {
    map<string, bufferlist> raw_data;
    int max_read = std::min<uint64_t>(RBD_MAX_KEYS_READ, max_return - data.size());
    int r = cls_cxx_map_get_vals(hctx, last_read, RBD_METADATA_KEY_PREFIX,
                                 max_read, &raw_data, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("failed to read the vals off of disk: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto& kv : raw_data) {
      data[metadata_name_from_key(kv.first)].swap(kv.second);
    }

    if (!raw_data.empty()) {
      last_read = raw_data.rbegin()->first;
    }
  }

  encode(data, *out);
  return 0;
}

/**
 * Input:
 * @param data <map(key, value)>
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int metadata_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  map<string, bufferlist> data, raw_data;

  auto iter = in->cbegin();
  try {
    decode(data, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  for (map<string, bufferlist>::iterator it = data.begin();
       it != data.end(); ++it) {
    CLS_LOG(20, "metadata_set key=%s value=%.*s", it->first.c_str(),
	    it->second.length(), it->second.c_str());
    raw_data[metadata_key_for_name(it->first)].swap(it->second);
  }
  int r = cls_cxx_map_set_vals(hctx, &raw_data);
  if (r < 0) {
    CLS_ERR("error writing metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/**
 * Input:
 * @param key
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int metadata_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string key;

  auto iter = in->cbegin();
  try {
    decode(key, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "metadata_remove key=%s", key.c_str());

  int r = cls_cxx_map_remove_key(hctx, metadata_key_for_name(key));
  if (r < 0) {
    CLS_ERR("error removing metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/**
 * Input:
 * @param key
 *
 * Output:
 * @param metadata value associated with the key
 * @returns 0 on success, negative error code on failure
 */
int metadata_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string key;
  bufferlist value;

  auto iter = in->cbegin();
  try {
    decode(key, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "metadata_get key=%s", key.c_str());

  int r = cls_cxx_map_get_val(hctx, metadata_key_for_name(key), &value);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error getting metadata: %s", cpp_strerror(r).c_str());
    return r;
  }

  encode(value, *out);
  return 0;
}

int snapshot_get_limit(cls_method_context_t hctx, bufferlist *in,
		       bufferlist *out)
{
  uint64_t snap_limit;
  int r = read_key(hctx, "snap_limit", &snap_limit);
  if (r == -ENOENT) {
    snap_limit = UINT64_MAX;
  } else if (r < 0) {
    CLS_ERR("error retrieving snapshot limit: %s", cpp_strerror(r).c_str());
    return r;
  }

  CLS_LOG(20, "read snapshot limit %" PRIu64, snap_limit);
  encode(snap_limit, *out);

  return 0;
}

int snapshot_set_limit(cls_method_context_t hctx, bufferlist *in,
		       bufferlist *out)
{
  int rc;
  uint64_t new_limit;
  bufferlist bl;
  size_t snap_count = 0;

  try {
    auto iter = in->cbegin();
    decode(new_limit, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (new_limit == UINT64_MAX) {
    CLS_LOG(20, "remove snapshot limit\n");
    rc = cls_cxx_map_remove_key(hctx, "snap_limit");
    return rc;
  }

  //try to read header as v1 format
  rc = snap_read_header(hctx, bl);

  // error when reading header
  if (rc < 0 && rc != -EINVAL) {
    return rc;
  } else if (rc >= 0) {
    // success, the image is v1 format
    struct rbd_obj_header_ondisk *header;
    header = (struct rbd_obj_header_ondisk *)bl.c_str();
    snap_count = header->snap_count;
  } else {
    // else, the image is v2 format
    int max_read = RBD_MAX_KEYS_READ;
    string last_read = RBD_SNAP_KEY_PREFIX;
    bool more;

    do {
      set<string> keys;
      rc = cls_cxx_map_get_keys(hctx, last_read, max_read, &keys, &more);
      if (rc < 0) {
        CLS_ERR("error retrieving snapshots: %s", cpp_strerror(rc).c_str());
        return rc;
      }
      for (auto& key : keys) {
        if (key.find(RBD_SNAP_KEY_PREFIX) != 0)
          break;
        snap_count++;
      }
      if (!keys.empty())
        last_read = *(keys.rbegin());
    } while (more);
  }

  if (new_limit < snap_count) {
    rc = -ERANGE;
    CLS_LOG(10, "snapshot limit is less than the number of snapshots.\n");
  } else {
    CLS_LOG(20, "set snapshot limit to %" PRIu64 "\n", new_limit);
    bl.clear();
    encode(new_limit, bl);
    rc = cls_cxx_map_set_val(hctx, "snap_limit", &bl);
  }

  return rc;
}


/**
 * Input:
 * @param snap id (uint64_t) parent snapshot id
 * @param child spec (cls::rbd::ChildImageSpec) child image
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int child_attach(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;
  cls::rbd::ChildImageSpec child_image;
  try {
    auto it = in->cbegin();
    decode(snap_id, it);
    decode(child_image, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "child_attach snap_id=%" PRIu64 ", child_pool_id=%" PRIi64 ", "
              "child_image_id=%s", snap_id, child_image.pool_id,
               child_image.image_id.c_str());

  cls_rbd_snap snap;
  std::string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    return r;
  }

  if (cls::rbd::get_snap_namespace_type(snap.snapshot_namespace) ==
        cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
    // cannot attach to a deleted snapshot
    return -ENOENT;
  }

  auto children_key = image::snap_children_key_from_snap_id(snap_id);
  cls::rbd::ChildImageSpecs child_images;
  r = read_key(hctx, children_key, &child_images);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error reading snapshot children: %s", cpp_strerror(r).c_str());
    return r;
  }

  auto it = child_images.insert(child_image);
  if (!it.second) {
    // child already attached to the snapshot
    return -EEXIST;
  }

  r = write_key(hctx, children_key, child_images);
  if (r < 0) {
    CLS_ERR("error writing snapshot children: %s", cpp_strerror(r).c_str());
    return r;
  }

  ++snap.child_count;
  r = image::snapshot::write(hctx, snapshot_key, std::move(snap));
  if (r < 0) {
    return r;
  }

  r = image::set_op_features(hctx, RBD_OPERATION_FEATURE_CLONE_PARENT,
                             RBD_OPERATION_FEATURE_CLONE_PARENT);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Input:
 * @param snap id (uint64_t) parent snapshot id
 * @param child spec (cls::rbd::ChildImageSpec) child image
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int child_detach(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;
  cls::rbd::ChildImageSpec child_image;
  try {
    auto it = in->cbegin();
    decode(snap_id, it);
    decode(child_image, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "child_detach snap_id=%" PRIu64 ", child_pool_id=%" PRIi64 ", "
              "child_image_id=%s", snap_id, child_image.pool_id,
               child_image.image_id.c_str());

  cls_rbd_snap snap;
  std::string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    return r;
  }

  auto children_key = image::snap_children_key_from_snap_id(snap_id);
  cls::rbd::ChildImageSpecs child_images;
  r = read_key(hctx, children_key, &child_images);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error reading snapshot children: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (snap.child_count != child_images.size()) {
    // children and reference count don't match
    CLS_ERR("children reference count mismatch: %" PRIu64, snap_id);
    return -EINVAL;
  }

  if (child_images.erase(child_image) == 0) {
    // child not attached to the snapshot
    return -ENOENT;
  }

  if (child_images.empty()) {
    r = remove_key(hctx, children_key);
  } else {
    r = write_key(hctx, children_key, child_images);
    if (r < 0) {
      CLS_ERR("error writing snapshot children: %s", cpp_strerror(r).c_str());
      return r;
    }
  }

  --snap.child_count;
  r = image::snapshot::write(hctx, snapshot_key, std::move(snap));
  if (r < 0) {
    return r;
  }

  if (snap.child_count == 0) {
    auto clone_in_use_lambda = [snap_id](const cls_rbd_snap& snap_meta) {
      if (snap_meta.id != snap_id && snap_meta.child_count > 0) {
        return -EEXIST;
      }
      return 0;
    };

    r = image::snapshot::iterate(hctx, clone_in_use_lambda);
    if (r < 0 && r != -EEXIST) {
      return r;
    }

    if (r != -EEXIST) {
      // remove the clone_v2 op feature if not in-use by any other snapshots
      r = image::set_op_features(hctx, 0, RBD_OPERATION_FEATURE_CLONE_PARENT);
      if (r < 0) {
        return r;
      }
    }
  }

  return 0;
}

/**
 * Input:
 * @param snap id (uint64_t) parent snapshot id
 *
 * Output:
 * @param (cls::rbd::ChildImageSpecs) child images
 * @returns 0 on success, negative error code on failure
 */
int children_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;
  try {
    auto it = in->cbegin();
    decode(snap_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "child_detach snap_id=%" PRIu64, snap_id);

  cls_rbd_snap snap;
  std::string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0) {
    return r;
  }

  auto children_key = image::snap_children_key_from_snap_id(snap_id);
  cls::rbd::ChildImageSpecs child_images;
  r = read_key(hctx, children_key, &child_images);
  if (r == -ENOENT) {
    return r;
  } else if (r < 0) {
    CLS_ERR("error reading snapshot children: %s", cpp_strerror(r).c_str());
    return r;
  }

  encode(child_images, *out);
  return 0;
}

/**
 * Set image migration.
 *
 * Input:
 * @param migration_spec (cls::rbd::MigrationSpec) image migration spec
 *
 * Output:
 *
 * @returns 0 on success, negative error code on failure
 */
int migration_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls::rbd::MigrationSpec migration_spec;
  try {
    auto it = in->cbegin();
    decode(migration_spec, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = image::set_migration(hctx, migration_spec, true);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Set image migration state.
 *
 * Input:
 * @param state (cls::rbd::MigrationState) migration state
 * @param description (std::string) migration state description
 *
 * Output:
 *
 * @returns 0 on success, negative error code on failure
 */
int migration_set_state(cls_method_context_t hctx, bufferlist *in,
                        bufferlist *out) {
  cls::rbd::MigrationState state;
  std::string description;
  try {
    auto it = in->cbegin();
    decode(state, it);
    decode(description, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  cls::rbd::MigrationSpec migration_spec;
  int r = image::read_migration(hctx, &migration_spec);
  if (r < 0) {
    return r;
  }

  migration_spec.state = state;
  migration_spec.state_description = description;

  r = image::set_migration(hctx, migration_spec, false);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Get image migration spec.
 *
 * Input:
 *
 * Output:
 * @param migration_spec (cls::rbd::MigrationSpec) image migration spec
 *
 * @returns 0 on success, negative error code on failure
 */
int migration_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls::rbd::MigrationSpec migration_spec;
  int r = image::read_migration(hctx, &migration_spec);
  if (r < 0) {
    return r;
  }

  encode(migration_spec, *out);

  return 0;
}

/**
 * Remove image migration spec.
 *
 * Input:
 *
 * Output:
 *
 * @returns 0 on success, negative error code on failure
 */
int migration_remove(cls_method_context_t hctx, bufferlist *in,
                     bufferlist *out) {
  int r = image::remove_migration(hctx);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Ensure writer snapc state
 *
 * Input:
 * @param snap id (uint64_t) snap context sequence id
 * @param state (cls::rbd::AssertSnapcSeqState) snap context state
 *
 * Output:
 * @returns -ERANGE if assertion fails
 * @returns 0 on success, negative error code on failure
 */
int assert_snapc_seq(cls_method_context_t hctx, bufferlist *in,
                     bufferlist *out)
{
  uint64_t snapc_seq;
  cls::rbd::AssertSnapcSeqState state;
  try {
    auto it = in->cbegin();
    decode(snapc_seq, it);
    decode(state, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  uint64_t snapset_seq;
  int r = cls_get_snapset_seq(hctx, &snapset_seq);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  switch (state) {
  case cls::rbd::ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ:
    return (r == -ENOENT || snapc_seq > snapset_seq) ? 0 : -ERANGE;
  case cls::rbd::ASSERT_SNAPC_SEQ_LE_SNAPSET_SEQ:
    return (r == -ENOENT || snapc_seq > snapset_seq) ? -ERANGE : 0;
  default:
    return -EOPNOTSUPP;
  }
}

/****************************** Old format *******************************/

int old_snapshots_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();
  bufferptr p(header->snap_names_len);
  char *buf = (char *)header;
  char *name = buf + sizeof(*header) + header->snap_count * sizeof(struct rbd_obj_snap_ondisk);
  char *end = name + header->snap_names_len;
  memcpy(p.c_str(),
         buf + sizeof(*header) + header->snap_count * sizeof(struct rbd_obj_snap_ondisk),
         header->snap_names_len);

  encode(header->snap_seq, *out);
  encode(header->snap_count, *out);

  for (unsigned i = 0; i < header->snap_count; i++) {
    string s = name;
    encode(header->snaps[i].id, *out);
    encode(header->snaps[i].image_size, *out);
    encode(s, *out);

    name += strlen(name) + 1;
    if (name > end)
      return -EIO;
  }

  return 0;
}

int old_snapshot_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  bufferlist newbl;
  bufferptr header_bp(sizeof(*header));
  struct rbd_obj_snap_ondisk *new_snaps;

  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();

  int snaps_id_ofs = sizeof(*header);
  int names_ofs = snaps_id_ofs + sizeof(*new_snaps) * header->snap_count;
  const char *snap_name;
  const char *snap_names = ((char *)header) + names_ofs;
  const char *end = snap_names + header->snap_names_len;
  auto iter = in->cbegin();
  string s;
  uint64_t snap_id;

  try {
    decode(s, iter);
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  snap_name = s.c_str();

  if (header->snap_seq > snap_id)
    return -ESTALE;

  uint64_t snap_limit;
  rc = read_key(hctx, "snap_limit", &snap_limit);
  if (rc == -ENOENT) {
    snap_limit = UINT64_MAX;
  } else if (rc < 0) {
    return rc;
  }

  if (header->snap_count >= snap_limit)
    return -EDQUOT;

  const char *cur_snap_name;
  for (cur_snap_name = snap_names; cur_snap_name < end; cur_snap_name += strlen(cur_snap_name) + 1) {
    if (strncmp(cur_snap_name, snap_name, end - cur_snap_name) == 0)
      return -EEXIST;
  }
  if (cur_snap_name > end)
    return -EIO;

  int snap_name_len = strlen(snap_name);

  bufferptr new_names_bp(header->snap_names_len + snap_name_len + 1);
  bufferptr new_snaps_bp(sizeof(*new_snaps) * (header->snap_count + 1));

  /* copy snap names and append to new snap name */
  char *new_snap_names = new_names_bp.c_str();
  strcpy(new_snap_names, snap_name);
  memcpy(new_snap_names + snap_name_len + 1, snap_names, header->snap_names_len);

  /* append new snap id */
  new_snaps = (struct rbd_obj_snap_ondisk *)new_snaps_bp.c_str();
  memcpy(new_snaps + 1, header->snaps, sizeof(*new_snaps) * header->snap_count);

  header->snap_count = header->snap_count + 1;
  header->snap_names_len = header->snap_names_len + snap_name_len + 1;
  header->snap_seq = snap_id;

  new_snaps[0].id = snap_id;
  new_snaps[0].image_size = header->image_size;

  memcpy(header_bp.c_str(), header, sizeof(*header));

  newbl.push_back(header_bp);
  newbl.push_back(new_snaps_bp);
  newbl.push_back(new_names_bp);

  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0)
    return rc;

  return 0;
}

int old_snapshot_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  bufferlist newbl;
  bufferptr header_bp(sizeof(*header));

  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();

  int snaps_id_ofs = sizeof(*header);
  int names_ofs = snaps_id_ofs + sizeof(struct rbd_obj_snap_ondisk) * header->snap_count;
  const char *snap_name;
  const char *snap_names = ((char *)header) + names_ofs;
  const char *orig_names = snap_names;
  const char *end = snap_names + header->snap_names_len;
  auto iter = in->cbegin();
  string s;
  unsigned i;
  bool found = false;
  struct rbd_obj_snap_ondisk snap;

  try {
    decode(s, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  snap_name = s.c_str();

  for (i = 0; snap_names < end; i++) {
    if (strcmp(snap_names, snap_name) == 0) {
      snap = header->snaps[i];
      found = true;
      break;
    }
    snap_names += strlen(snap_names) + 1;
  }
  if (!found) {
    CLS_ERR("couldn't find snap %s\n", snap_name);
    return -ENOENT;
  }

  header->snap_names_len  = header->snap_names_len - (s.length() + 1);
  header->snap_count = header->snap_count - 1;

  bufferptr new_names_bp(header->snap_names_len);
  bufferptr new_snaps_bp(sizeof(header->snaps[0]) * header->snap_count);

  memcpy(header_bp.c_str(), header, sizeof(*header));
  newbl.push_back(header_bp);

  if (header->snap_count) {
    int snaps_len = 0;
    int names_len = 0;
    CLS_LOG(20, "i=%u\n", i);
    if (i > 0) {
      snaps_len = sizeof(header->snaps[0]) * i;
      names_len =  snap_names - orig_names;
      memcpy(new_snaps_bp.c_str(), header->snaps, snaps_len);
      memcpy(new_names_bp.c_str(), orig_names, names_len);
    }
    snap_names += s.length() + 1;

    if (i < header->snap_count) {
      memcpy(new_snaps_bp.c_str() + snaps_len,
             header->snaps + i + 1,
             sizeof(header->snaps[0]) * (header->snap_count - i));
      memcpy(new_names_bp.c_str() + names_len, snap_names , end - snap_names);
    }
    newbl.push_back(new_snaps_bp);
    newbl.push_back(new_names_bp);
  }

  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0)
    return rc;

  return 0;
}

/**
 * rename snapshot of old format.
 *
 * Input:
 * @param src_snap_id old snap id of the snapshot (snapid_t)
 * @param dst_snap_name new name of the snapshot (string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure.
*/
int old_snapshot_rename(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  bufferlist newbl;
  bufferptr header_bp(sizeof(*header));
  snapid_t src_snap_id;
  const char *dst_snap_name;
  string dst;

  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();

  int snaps_id_ofs = sizeof(*header);
  int names_ofs = snaps_id_ofs + sizeof(rbd_obj_snap_ondisk) * header->snap_count;
  const char *snap_names = ((char *)header) + names_ofs;
  const char *orig_names = snap_names;
  const char *end = snap_names + header->snap_names_len;
  auto iter = in->cbegin();
  unsigned i;
  bool found = false;

  try {
    decode(src_snap_id, iter);
    decode(dst, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  dst_snap_name = dst.c_str();

  const char *cur_snap_name;
  for (cur_snap_name = snap_names; cur_snap_name < end; 
    cur_snap_name += strlen(cur_snap_name) + 1) {
    if (strcmp(cur_snap_name, dst_snap_name) == 0)
      return -EEXIST;
  }
  if (cur_snap_name > end)
    return -EIO;
  for (i = 0; i < header->snap_count; i++) {
    if (src_snap_id == header->snaps[i].id) {
      found = true;
      break;
    }
    snap_names += strlen(snap_names) + 1;
  }
  if (!found) {
    CLS_ERR("couldn't find snap %llu\n", (unsigned long long)src_snap_id.val);
    return -ENOENT;
  }
  
  CLS_LOG(20, "rename snap with snap id %llu to dest name %s", (unsigned long long)src_snap_id.val, dst_snap_name);
  header->snap_names_len  = header->snap_names_len - strlen(snap_names) + dst.length();

  bufferptr new_names_bp(header->snap_names_len);
  bufferptr new_snaps_bp(sizeof(header->snaps[0]) * header->snap_count);

  if (header->snap_count) {
    int names_len = 0;
    CLS_LOG(20, "i=%u\n", i);
    if (i > 0) {
      names_len =  snap_names - orig_names;
      memcpy(new_names_bp.c_str(), orig_names, names_len);
    }
    strcpy(new_names_bp.c_str() + names_len, dst_snap_name);
    names_len += strlen(dst_snap_name) + 1;
    snap_names += strlen(snap_names) + 1;
    if (i < header->snap_count) {
      memcpy(new_names_bp.c_str() + names_len, snap_names , end - snap_names);
    }
    memcpy(new_snaps_bp.c_str(), header->snaps, sizeof(header->snaps[0]) * header->snap_count);
  }

  memcpy(header_bp.c_str(), header, sizeof(*header));
  newbl.push_back(header_bp);
  newbl.push_back(new_snaps_bp);
  newbl.push_back(new_names_bp);

  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0)
    return rc;
  return 0;
}


namespace mirror {

static const std::string UUID("mirror_uuid");
static const std::string MODE("mirror_mode");
static const std::string PEER_KEY_PREFIX("mirror_peer_");
static const std::string IMAGE_KEY_PREFIX("image_");
static const std::string GLOBAL_KEY_PREFIX("global_");
static const std::string STATUS_GLOBAL_KEY_PREFIX("status_global_");
static const std::string INSTANCE_KEY_PREFIX("instance_");
static const std::string MIRROR_IMAGE_MAP_KEY_PREFIX("image_map_");

std::string peer_key(const std::string &uuid) {
  return PEER_KEY_PREFIX + uuid;
}

std::string image_key(const string &image_id) {
  return IMAGE_KEY_PREFIX + image_id;
}

std::string global_key(const string &global_id) {
  return GLOBAL_KEY_PREFIX + global_id;
}

std::string status_global_key(const string &global_id) {
  return STATUS_GLOBAL_KEY_PREFIX + global_id;
}

std::string instance_key(const string &instance_id) {
  return INSTANCE_KEY_PREFIX + instance_id;
}

std::string mirror_image_map_key(const string& global_image_id) {
  return MIRROR_IMAGE_MAP_KEY_PREFIX + global_image_id;
}

int uuid_get(cls_method_context_t hctx, std::string *mirror_uuid) {
  bufferlist mirror_uuid_bl;
  int r = cls_cxx_map_get_val(hctx, mirror::UUID, &mirror_uuid_bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading mirror uuid: %s", cpp_strerror(r).c_str());
    }
    return r;
  }

  *mirror_uuid = std::string(mirror_uuid_bl.c_str(), mirror_uuid_bl.length());
  return 0;
}

int list_watchers(cls_method_context_t hctx,
                  std::set<entity_inst_t> *entities) {
  obj_list_watch_response_t watchers;
  int r = cls_cxx_list_watchers(hctx, &watchers);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error listing watchers: '%s'", cpp_strerror(r).c_str());
    return r;
  }

  entities->clear();
  for (auto &w : watchers.entries) {
    entities->emplace(w.name, w.addr);
  }
  return 0;
}

int read_peers(cls_method_context_t hctx,
               std::vector<cls::rbd::MirrorPeer> *peers) {
  std::string last_read = PEER_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  while (more) {
    std::map<std::string, bufferlist> vals;
    int r = cls_cxx_map_get_vals(hctx, last_read, PEER_KEY_PREFIX.c_str(),
                                 max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading peers: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto &it : vals) {
      try {
        auto bl_it = it.second.cbegin();
        cls::rbd::MirrorPeer peer;
	decode(peer, bl_it);
        peers->push_back(peer);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode peer '%s'", it.first.c_str());
	return -EIO;
      }
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }
  return 0;
}

int read_peer(cls_method_context_t hctx, const std::string &id,
              cls::rbd::MirrorPeer *peer) {
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, peer_key(id), &bl);
  if (r < 0) {
    CLS_ERR("error reading peer '%s': %s", id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }

  try {
    auto bl_it = bl.cbegin();
    decode(*peer, bl_it);
  } catch (const buffer::error &err) {
    CLS_ERR("could not decode peer '%s'", id.c_str());
    return -EIO;
  }
  return 0;
}

int write_peer(cls_method_context_t hctx, const std::string &id,
               const cls::rbd::MirrorPeer &peer) {
  bufferlist bl;
  encode(peer, bl);

  int r = cls_cxx_map_set_val(hctx, peer_key(id), &bl);
  if (r < 0) {
    CLS_ERR("error writing peer '%s': %s", id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int image_get(cls_method_context_t hctx, const string &image_id,
	      cls::rbd::MirrorImage *mirror_image) {
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, image_key(image_id), &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading mirrored image '%s': '%s'", image_id.c_str(),
	      cpp_strerror(r).c_str());
    }
    return r;
  }

  try {
    auto it = bl.cbegin();
    decode(*mirror_image, it);
  } catch (const buffer::error &err) {
    CLS_ERR("could not decode mirrored image '%s'", image_id.c_str());
    return -EIO;
  }

  return 0;
}

int image_set(cls_method_context_t hctx, const string &image_id,
	      const cls::rbd::MirrorImage &mirror_image) {
  bufferlist bl;
  encode(mirror_image, bl);

  cls::rbd::MirrorImage existing_mirror_image;
  int r = image_get(hctx, image_id, &existing_mirror_image);
  if (r == -ENOENT) {
    // make sure global id doesn't already exist
    std::string global_id_key = global_key(mirror_image.global_image_id);
    std::string image_id;
    r = read_key(hctx, global_id_key, &image_id);
    if (r >= 0) {
      return -EEXIST;
    } else if (r != -ENOENT) {
      CLS_ERR("error reading global image id: '%s': '%s'", image_id.c_str(),
              cpp_strerror(r).c_str());
      return r;
    }

    // make sure this was not a race for disabling
    if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
      CLS_ERR("image '%s' is already disabled", image_id.c_str());
      return r;
    }
  } else if (r < 0) {
    CLS_ERR("error reading mirrored image '%s': '%s'", image_id.c_str(),
	    cpp_strerror(r).c_str());
    return r;
  } else if (existing_mirror_image.global_image_id !=
                mirror_image.global_image_id) {
    // cannot change the global id
    return -EINVAL;
  }

  r = cls_cxx_map_set_val(hctx, image_key(image_id), &bl);
  if (r < 0) {
    CLS_ERR("error adding mirrored image '%s': %s", image_id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }

  bufferlist image_id_bl;
  encode(image_id, image_id_bl);
  r = cls_cxx_map_set_val(hctx, global_key(mirror_image.global_image_id),
                          &image_id_bl);
  if (r < 0) {
    CLS_ERR("error adding global id for image '%s': %s", image_id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int image_remove(cls_method_context_t hctx, const string &image_id) {
  bufferlist bl;
  cls::rbd::MirrorImage mirror_image;
  int r = image_get(hctx, image_id, &mirror_image);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading mirrored image '%s': '%s'", image_id.c_str(),
	      cpp_strerror(r).c_str());
    }
    return r;
  }

  if (mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_DISABLING) {
    return -EBUSY;
  }

  r = cls_cxx_map_remove_key(hctx, image_key(image_id));
  if (r < 0) {
    CLS_ERR("error removing mirrored image '%s': %s", image_id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, global_key(mirror_image.global_image_id));
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error removing global id for image '%s': %s", image_id.c_str(),
           cpp_strerror(r).c_str());
    return r;
  }

  r = cls_cxx_map_remove_key(hctx,
                             status_global_key(mirror_image.global_image_id));
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error removing global status for image '%s': %s", image_id.c_str(),
           cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

struct MirrorImageStatusOnDisk : cls::rbd::MirrorImageStatus {
  entity_inst_t origin;

  MirrorImageStatusOnDisk() {
  }
  MirrorImageStatusOnDisk(const cls::rbd::MirrorImageStatus &status) :
    cls::rbd::MirrorImageStatus(status) {
  }

  void encode_meta(bufferlist &bl, uint64_t features) const {
    ENCODE_START(1, 1, bl);
    encode(origin, bl, features);
    ENCODE_FINISH(bl);
  }

  void encode(bufferlist &bl, uint64_t features) const {
    encode_meta(bl, features);
    cls::rbd::MirrorImageStatus::encode(bl);
  }

  void decode_meta(bufferlist::const_iterator &it) {
    DECODE_START(1, it);
    decode(origin, it);
    DECODE_FINISH(it);
  }

  void decode(bufferlist::const_iterator &it) {
    decode_meta(it);
    cls::rbd::MirrorImageStatus::decode(it);
  }
};
WRITE_CLASS_ENCODER_FEATURES(MirrorImageStatusOnDisk)

int image_status_set(cls_method_context_t hctx, const string &global_image_id,
		     const cls::rbd::MirrorImageStatus &status) {
  MirrorImageStatusOnDisk ondisk_status(status);
  ondisk_status.up = false;
  ondisk_status.last_update = ceph_clock_now();

  int r = cls_get_request_origin(hctx, &ondisk_status.origin);
  ceph_assert(r == 0);

  bufferlist bl;
  encode(ondisk_status, bl, cls_get_features(hctx));

  r = cls_cxx_map_set_val(hctx, status_global_key(global_image_id), &bl);
  if (r < 0) {
    CLS_ERR("error setting status for mirrored image, global id '%s': %s",
	    global_image_id.c_str(), cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int image_status_remove(cls_method_context_t hctx,
			const string &global_image_id) {

  int r = cls_cxx_map_remove_key(hctx, status_global_key(global_image_id));
  if (r < 0) {
    CLS_ERR("error removing status for mirrored image, global id '%s': %s",
	    global_image_id.c_str(), cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int image_status_get(cls_method_context_t hctx, const string &global_image_id,
                     const std::set<entity_inst_t> &watchers,
		     cls::rbd::MirrorImageStatus *status) {

  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, status_global_key(global_image_id), &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading status for mirrored image, global id '%s': '%s'",
	      global_image_id.c_str(), cpp_strerror(r).c_str());
    }
    return r;
  }

  MirrorImageStatusOnDisk ondisk_status;
  try {
    auto it = bl.cbegin();
    decode(ondisk_status, it);
  } catch (const buffer::error &err) {
    CLS_ERR("could not decode status for mirrored image, global id '%s'",
	    global_image_id.c_str());
    return -EIO;
  }


  *status = static_cast<cls::rbd::MirrorImageStatus>(ondisk_status);
  status->up = (watchers.find(ondisk_status.origin) != watchers.end());
  return 0;
}

int image_status_list(cls_method_context_t hctx,
	const std::string &start_after, uint64_t max_return,
	map<std::string, cls::rbd::MirrorImage> *mirror_images,
        map<std::string, cls::rbd::MirrorImageStatus> *mirror_statuses) {
  std::string last_read = image_key(start_after);
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;

  std::set<entity_inst_t> watchers;
  int r = list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  while (more && mirror_images->size() < max_return) {
    std::map<std::string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    r = cls_cxx_map_get_vals(hctx, last_read, IMAGE_KEY_PREFIX, max_read, &vals,
                             &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading mirror image directory by name: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto it = vals.begin(); it != vals.end() &&
	   mirror_images->size() < max_return; ++it) {
      const std::string &image_id = it->first.substr(IMAGE_KEY_PREFIX.size());
      cls::rbd::MirrorImage mirror_image;
      auto iter = it->second.cbegin();
      try {
	decode(mirror_image, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode mirror image payload of image '%s'",
                image_id.c_str());
	return -EIO;
      }

      (*mirror_images)[image_id] = mirror_image;

      cls::rbd::MirrorImageStatus status;
      int r1 = image_status_get(hctx, mirror_image.global_image_id, watchers,
                                &status);
      if (r1 < 0) {
	continue;
      }

      (*mirror_statuses)[image_id] = status;
    }
    if (!vals.empty()) {
      last_read = image_key(mirror_images->rbegin()->first);
    }
  }

  return 0;
}

int image_status_get_summary(
    cls_method_context_t hctx,
    std::map<cls::rbd::MirrorImageStatusState, int> *states) {
  std::set<entity_inst_t> watchers;
  int r = list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  states->clear();

  string last_read = IMAGE_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  while (more) {
    map<string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, IMAGE_KEY_PREFIX,
			     max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading mirrored images: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto &list_it : vals) {
      const string &key = list_it.first;

      if (0 != key.compare(0, IMAGE_KEY_PREFIX.size(), IMAGE_KEY_PREFIX)) {
	break;
      }

      cls::rbd::MirrorImage mirror_image;
      auto iter = list_it.second.cbegin();
      try {
	decode(mirror_image, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode mirror image payload for key '%s'",
                key.c_str());
	return -EIO;
      }

      cls::rbd::MirrorImageStatus status;
      image_status_get(hctx, mirror_image.global_image_id, watchers, &status);

      cls::rbd::MirrorImageStatusState state = status.up ? status.state :
	cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN;
      (*states)[state]++;
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }

  return 0;
}

int image_status_remove_down(cls_method_context_t hctx) {
  std::set<entity_inst_t> watchers;
  int r = list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  string last_read = STATUS_GLOBAL_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  while (more) {
    map<string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, STATUS_GLOBAL_KEY_PREFIX,
			     max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading mirrored images: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto &list_it : vals) {
      const string &key = list_it.first;

      if (0 != key.compare(0, STATUS_GLOBAL_KEY_PREFIX.size(),
			   STATUS_GLOBAL_KEY_PREFIX)) {
	break;
      }

      MirrorImageStatusOnDisk status;
      try {
	auto it = list_it.second.cbegin();
	status.decode_meta(it);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode status metadata for mirrored image '%s'",
		key.c_str());
	return -EIO;
      }

      if (watchers.find(status.origin) == watchers.end()) {
	CLS_LOG(20, "removing stale status object for key %s",
		key.c_str());
	int r1 = cls_cxx_map_remove_key(hctx, key);
	if (r1 < 0) {
	  CLS_ERR("error removing stale status for key '%s': %s",
		  key.c_str(), cpp_strerror(r1).c_str());
	  return r1;
	}
      }
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }

  return 0;
}

int image_instance_get(cls_method_context_t hctx,
                       const string &global_image_id,
                       const std::set<entity_inst_t> &watchers,
                       entity_inst_t *instance) {
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, status_global_key(global_image_id), &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading status for mirrored image, global id '%s': '%s'",
              global_image_id.c_str(), cpp_strerror(r).c_str());
    }
    return r;
  }

  MirrorImageStatusOnDisk ondisk_status;
  try {
    auto it = bl.cbegin();
    decode(ondisk_status, it);
  } catch (const buffer::error &err) {
    CLS_ERR("could not decode status for mirrored image, global id '%s'",
            global_image_id.c_str());
    return -EIO;
  }

  if (watchers.find(ondisk_status.origin) == watchers.end()) {
    return -ESTALE;
  }

  *instance = ondisk_status.origin;
  return 0;
}

int image_instance_list(cls_method_context_t hctx,
                        const std::string &start_after,
                        uint64_t max_return,
                        map<std::string, entity_inst_t> *instances) {
  std::string last_read = image_key(start_after);
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;

  std::set<entity_inst_t> watchers;
  int r = list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  while (more && instances->size() < max_return) {
    std::map<std::string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    r = cls_cxx_map_get_vals(hctx, last_read, IMAGE_KEY_PREFIX, max_read, &vals,
                             &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading mirror image directory by name: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto it = vals.begin(); it != vals.end() &&
           instances->size() < max_return; ++it) {
      const std::string &image_id = it->first.substr(IMAGE_KEY_PREFIX.size());
      cls::rbd::MirrorImage mirror_image;
      auto iter = it->second.cbegin();
      try {
        decode(mirror_image, iter);
      } catch (const buffer::error &err) {
        CLS_ERR("could not decode mirror image payload of image '%s'",
                image_id.c_str());
        return -EIO;
      }

      entity_inst_t instance;
      r = image_instance_get(hctx, mirror_image.global_image_id, watchers,
                             &instance);
      if (r < 0) {
        continue;
      }

      (*instances)[image_id] = instance;
    }
    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }

  return 0;
}

int instances_list(cls_method_context_t hctx,
                   std::vector<std::string> *instance_ids) {
  std::string last_read = INSTANCE_KEY_PREFIX;
  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  while (more) {
    std::map<std::string, bufferlist> vals;
    int r = cls_cxx_map_get_vals(hctx, last_read, INSTANCE_KEY_PREFIX.c_str(),
                                 max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
	CLS_ERR("error reading mirror instances: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto &it : vals) {
      instance_ids->push_back(it.first.substr(INSTANCE_KEY_PREFIX.size()));
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }
  return 0;
}

int instances_add(cls_method_context_t hctx, const string &instance_id) {
  bufferlist bl;

  int r = cls_cxx_map_set_val(hctx, instance_key(instance_id), &bl);
  if (r < 0) {
    CLS_ERR("error setting mirror instance %s: %s", instance_id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int instances_remove(cls_method_context_t hctx, const string &instance_id) {

  int r = cls_cxx_map_remove_key(hctx, instance_key(instance_id));
  if (r < 0) {
    CLS_ERR("error removing mirror instance %s: %s", instance_id.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

int mirror_image_map_list(cls_method_context_t hctx,
                          const std::string &start_after,
                          uint64_t max_return,
                          std::map<std::string, cls::rbd::MirrorImageMap> *image_mapping) {
  bool more = true;
  std::string last_read = mirror_image_map_key(start_after);

  while (more && image_mapping->size() < max_return) {
    std::map<std::string, bufferlist> vals;
    CLS_LOG(20, "last read: '%s'", last_read.c_str());

    int max_read = std::min<uint64_t>(RBD_MAX_KEYS_READ, max_return - image_mapping->size());
    int r = cls_cxx_map_get_vals(hctx, last_read, MIRROR_IMAGE_MAP_KEY_PREFIX,
                                 max_read, &vals, &more);
    if (r < 0) {
      CLS_ERR("error reading image map: %s", cpp_strerror(r).c_str());
      return r;
    }

    if (vals.empty()) {
      return 0;
    }

    for (auto it = vals.begin(); it != vals.end(); ++it) {
      const std::string &global_image_id =
        it->first.substr(MIRROR_IMAGE_MAP_KEY_PREFIX.size());

      cls::rbd::MirrorImageMap mirror_image_map;
      auto iter = it->second.cbegin();
      try {
        decode(mirror_image_map, iter);
      } catch (const buffer::error &err) {
        CLS_ERR("could not decode image map payload: %s",
                cpp_strerror(r).c_str());
        return -EINVAL;
      }

      image_mapping->insert(std::make_pair(global_image_id, mirror_image_map));
    }

    if (!vals.empty()) {
      last_read = vals.rbegin()->first;
    }
  }

  return 0;
}

} // namespace mirror

/**
 * Input:
 * none
 *
 * Output:
 * @param uuid (std::string)
 * @returns 0 on success, negative error code on failure
 */
int mirror_uuid_get(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  std::string mirror_uuid;
  int r = mirror::uuid_get(hctx, &mirror_uuid);
  if (r < 0) {
    return r;
  }

  encode(mirror_uuid, *out);
  return 0;
}

/**
 * Input:
 * @param mirror_uuid (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_uuid_set(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  std::string mirror_uuid;
  try {
    auto bl_it = in->cbegin();
    decode(mirror_uuid, bl_it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (mirror_uuid.empty()) {
    CLS_ERR("cannot set empty mirror uuid");
    return -EINVAL;
  }

  uint32_t mirror_mode;
  int r = read_key(hctx, mirror::MODE, &mirror_mode);
  if (r < 0 && r != -ENOENT) {
    return r;
  } else if (r == 0 && mirror_mode != cls::rbd::MIRROR_MODE_DISABLED) {
    CLS_ERR("cannot set mirror uuid while mirroring enabled");
    return -EINVAL;
  }

  bufferlist mirror_uuid_bl;
  mirror_uuid_bl.append(mirror_uuid);
  r = cls_cxx_map_set_val(hctx, mirror::UUID, &mirror_uuid_bl);
  if (r < 0) {
    CLS_ERR("failed to set mirror uuid");
    return r;
  }
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param cls::rbd::MirrorMode (uint32_t)
 * @returns 0 on success, negative error code on failure
 */
int mirror_mode_get(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  uint32_t mirror_mode_decode;
  int r = read_key(hctx, mirror::MODE, &mirror_mode_decode);
  if (r < 0) {
    return r;
  }

  encode(mirror_mode_decode, *out);
  return 0;
}

/**
 * Input:
 * @param mirror_mode (cls::rbd::MirrorMode) (uint32_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_mode_set(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  uint32_t mirror_mode_decode;
  try {
    auto bl_it = in->cbegin();
    decode(mirror_mode_decode, bl_it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  bool enabled;
  switch (static_cast<cls::rbd::MirrorMode>(mirror_mode_decode)) {
  case cls::rbd::MIRROR_MODE_DISABLED:
    enabled = false;
    break;
  case cls::rbd::MIRROR_MODE_IMAGE:
  case cls::rbd::MIRROR_MODE_POOL:
    enabled = true;
    break;
  default:
    CLS_ERR("invalid mirror mode: %d", mirror_mode_decode);
    return -EINVAL;
  }

  int r;
  if (enabled) {
    std::string mirror_uuid;
    r = mirror::uuid_get(hctx, &mirror_uuid);
    if (r == -ENOENT) {
      return -EINVAL;
    } else if (r < 0) {
      return r;
    }

    bufferlist bl;
    encode(mirror_mode_decode, bl);

    r = cls_cxx_map_set_val(hctx, mirror::MODE, &bl);
    if (r < 0) {
      CLS_ERR("error enabling mirroring: %s", cpp_strerror(r).c_str());
      return r;
    }
  } else {
    std::vector<cls::rbd::MirrorPeer> peers;
    r = mirror::read_peers(hctx, &peers);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    if (!peers.empty()) {
      CLS_ERR("mirroring peers still registered");
      return -EBUSY;
    }

    r = remove_key(hctx, mirror::MODE);
    if (r < 0) {
      return r;
    }

    r = remove_key(hctx, mirror::UUID);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param std::vector<cls::rbd::MirrorPeer>: collection of peers
 * @returns 0 on success, negative error code on failure
 */
int mirror_peer_list(cls_method_context_t hctx, bufferlist *in,
                     bufferlist *out) {
  std::vector<cls::rbd::MirrorPeer> peers;
  int r = mirror::read_peers(hctx, &peers);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  encode(peers, *out);
  return 0;
}

/**
 * Input:
 * @param mirror_peer (cls::rbd::MirrorPeer)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_peer_add(cls_method_context_t hctx, bufferlist *in,
                    bufferlist *out) {
  cls::rbd::MirrorPeer mirror_peer;
  try {
    auto it = in->cbegin();
    decode(mirror_peer, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  uint32_t mirror_mode_decode;
  int r = read_key(hctx, mirror::MODE, &mirror_mode_decode);
  if (r < 0 && r != -ENOENT) {
    return r;
  } else if (r == -ENOENT ||
             mirror_mode_decode == cls::rbd::MIRROR_MODE_DISABLED) {
    CLS_ERR("mirroring must be enabled on the pool");
    return -EINVAL;
  } else if (!mirror_peer.is_valid()) {
    CLS_ERR("mirror peer is not valid");
    return -EINVAL;
  }

  std::string mirror_uuid;
  r = mirror::uuid_get(hctx, &mirror_uuid);
  if (r < 0) {
    CLS_ERR("error retrieving mirroring uuid: %s", cpp_strerror(r).c_str());
    return r;
  } else if (mirror_peer.uuid == mirror_uuid) {
    CLS_ERR("peer uuid '%s' matches pool mirroring uuid",
            mirror_uuid.c_str());
    return -EINVAL;
  }

  std::vector<cls::rbd::MirrorPeer> peers;
  r = mirror::read_peers(hctx, &peers);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  for (auto const &peer : peers) {
    if (peer.uuid == mirror_peer.uuid) {
      CLS_ERR("peer uuid '%s' already exists",
              peer.uuid.c_str());
      return -ESTALE;
    } else if (peer.cluster_name == mirror_peer.cluster_name &&
               (peer.pool_id == -1 || mirror_peer.pool_id == -1 ||
                peer.pool_id == mirror_peer.pool_id)) {
      CLS_ERR("peer cluster name '%s' already exists",
              peer.cluster_name.c_str());
      return -EEXIST;
    }
  }

  bufferlist bl;
  encode(mirror_peer, bl);
  r = cls_cxx_map_set_val(hctx, mirror::peer_key(mirror_peer.uuid),
                          &bl);
  if (r < 0) {
    CLS_ERR("error adding peer: %s", cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param uuid (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_peer_remove(cls_method_context_t hctx, bufferlist *in,
                       bufferlist *out) {
  std::string uuid;
  try {
    auto it = in->cbegin();
    decode(uuid, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = cls_cxx_map_remove_key(hctx, mirror::peer_key(uuid));
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error removing peer: %s", cpp_strerror(r).c_str());
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param uuid (std::string)
 * @param client_name (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_peer_set_client(cls_method_context_t hctx, bufferlist *in,
                           bufferlist *out) {
  std::string uuid;
  std::string client_name;
  try {
    auto it = in->cbegin();
    decode(uuid, it);
    decode(client_name, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  cls::rbd::MirrorPeer peer;
  int r = mirror::read_peer(hctx, uuid, &peer);
  if (r < 0) {
    return r;
  }

  peer.client_name = client_name;
  r = mirror::write_peer(hctx, uuid, peer);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param uuid (std::string)
 * @param cluster_name (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_peer_set_cluster(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string uuid;
  std::string cluster_name;
  try {
    auto it = in->cbegin();
    decode(uuid, it);
    decode(cluster_name, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  cls::rbd::MirrorPeer peer;
  int r = mirror::read_peer(hctx, uuid, &peer);
  if (r < 0) {
    return r;
  }

  peer.cluster_name = cluster_name;
  r = mirror::write_peer(hctx, uuid, peer);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param std::map<std::string, std::string>: local id to global id map
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_list(cls_method_context_t hctx, bufferlist *in,
		     bufferlist *out) {
  std::string start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  std::map<std::string, std::string> mirror_images;
  std::string last_read = mirror::image_key(start_after);

  while (more && mirror_images.size() < max_return) {
    std::map<std::string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    int r = cls_cxx_map_get_vals(hctx, last_read, mirror::IMAGE_KEY_PREFIX,
                                 max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading mirror image directory by name: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto it = vals.begin(); it != vals.end(); ++it) {
      const std::string &image_id =
        it->first.substr(mirror::IMAGE_KEY_PREFIX.size());
      cls::rbd::MirrorImage mirror_image;
      auto iter = it->second.cbegin();
      try {
	decode(mirror_image, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode mirror image payload of image '%s'",
                image_id.c_str());
	return -EIO;
      }

      mirror_images[image_id] = mirror_image.global_image_id;
      if (mirror_images.size() >= max_return) {
	break;
      }
    }
    if (!vals.empty()) {
      last_read = mirror::image_key(mirror_images.rbegin()->first);
    }
  }

  encode(mirror_images, *out);
  return 0;
}

/**
 * Input:
 * @param global_id (std::string)
 *
 * Output:
 * @param std::string - image id
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_get_image_id(cls_method_context_t hctx, bufferlist *in,
                              bufferlist *out) {
  std::string global_id;
  try {
    auto it = in->cbegin();
    decode(global_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::string image_id;
  int r = read_key(hctx, mirror::global_key(global_id), &image_id);
  if (r < 0) {
    CLS_ERR("error retrieving image id for global id '%s': %s",
            global_id.c_str(), cpp_strerror(r).c_str());
    return r;
  }

  encode(image_id, *out);
  return 0;
}

/**
 * Input:
 * @param image_id (std::string)
 *
 * Output:
 * @param cls::rbd::MirrorImage - metadata associated with the image_id
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_get(cls_method_context_t hctx, bufferlist *in,
		     bufferlist *out) {
  string image_id;
  try {
    auto it = in->cbegin();
    decode(image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  cls::rbd::MirrorImage mirror_image;
  int r = mirror::image_get(hctx, image_id, &mirror_image);
  if (r < 0) {
    return r;
  }

  encode(mirror_image, *out);
  return 0;
}

/**
 * Input:
 * @param image_id (std::string)
 * @param mirror_image (cls::rbd::MirrorImage)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 * @returns -EEXIST if there's an existing image_id with a different global_image_id
 */
int mirror_image_set(cls_method_context_t hctx, bufferlist *in,
		     bufferlist *out) {
  string image_id;
  cls::rbd::MirrorImage mirror_image;
  try {
    auto it = in->cbegin();
    decode(image_id, it);
    decode(mirror_image, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::image_set(hctx, image_id, mirror_image);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param image_id (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_remove(cls_method_context_t hctx, bufferlist *in,
			bufferlist *out) {
  string image_id;
  try {
    auto it = in->cbegin();
    decode(image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::image_remove(hctx, image_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param global_image_id (std::string)
 * @param status (cls::rbd::MirrorImageStatus)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_set(cls_method_context_t hctx, bufferlist *in,
			    bufferlist *out) {
  string global_image_id;
  cls::rbd::MirrorImageStatus status;
  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
    decode(status, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::image_status_set(hctx, global_image_id, status);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param global_image_id (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_remove(cls_method_context_t hctx, bufferlist *in,
			       bufferlist *out) {
  string global_image_id;
  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::image_status_remove(hctx, global_image_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param global_image_id (std::string)
 *
 * Output:
 * @param cls::rbd::MirrorImageStatus - metadata associated with the global_image_id
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_get(cls_method_context_t hctx, bufferlist *in,
			    bufferlist *out) {
  string global_image_id;
  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::set<entity_inst_t> watchers;
  int r = mirror::list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  cls::rbd::MirrorImageStatus status;
  r = mirror::image_status_get(hctx, global_image_id, watchers, &status);
  if (r < 0) {
    return r;
  }

  encode(status, *out);
  return 0;
}

/**
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param std::map<std::string, cls::rbd::MirrorImage>: image id to image map
 * @param std::map<std::string, cls::rbd::MirrorImageStatus>: image it to status map
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_list(cls_method_context_t hctx, bufferlist *in,
			     bufferlist *out) {
  std::string start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  map<std::string, cls::rbd::MirrorImage> images;
  map<std::string, cls::rbd::MirrorImageStatus> statuses;
  int r = mirror::image_status_list(hctx, start_after, max_return, &images,
				    &statuses);
  if (r < 0) {
    return r;
  }

  encode(images, *out);
  encode(statuses, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param std::map<cls::rbd::MirrorImageStatusState, int>: states counts
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_get_summary(cls_method_context_t hctx, bufferlist *in,
				    bufferlist *out) {
  std::map<cls::rbd::MirrorImageStatusState, int> states;

  int r = mirror::image_status_get_summary(hctx, &states);
  if (r < 0) {
    return r;
  }

  encode(states, *out);
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_status_remove_down(cls_method_context_t hctx, bufferlist *in,
				    bufferlist *out) {
  int r = mirror::image_status_remove_down(hctx);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param global_image_id (std::string)
 *
 * Output:
 * @param entity_inst_t - instance
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_instance_get(cls_method_context_t hctx, bufferlist *in,
                              bufferlist *out) {
  string global_image_id;
  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::set<entity_inst_t> watchers;
  int r = mirror::list_watchers(hctx, &watchers);
  if (r < 0) {
    return r;
  }

  entity_inst_t instance;
  r = mirror::image_instance_get(hctx, global_image_id, watchers, &instance);
  if (r < 0) {
    return r;
  }

  encode(instance, *out, cls_get_features(hctx));
  return 0;
}

/**
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param std::map<std::string, entity_inst_t>: image id to instance map
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_instance_list(cls_method_context_t hctx, bufferlist *in,
                               bufferlist *out) {
  std::string start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  map<std::string, entity_inst_t> instances;
  int r = mirror::image_instance_list(hctx, start_after, max_return,
                                      &instances);
  if (r < 0) {
    return r;
  }

  encode(instances, *out, cls_get_features(hctx));
  return 0;
}

/**
 * Input:
 * none
 *
 * Output:
 * @param std::vector<std::string>: instance ids
 * @returns 0 on success, negative error code on failure
 */
int mirror_instances_list(cls_method_context_t hctx, bufferlist *in,
                          bufferlist *out) {
  std::vector<std::string> instance_ids;

  int r = mirror::instances_list(hctx, &instance_ids);
  if (r < 0) {
    return r;
  }

  encode(instance_ids, *out);
  return 0;
}

/**
 * Input:
 * @param instance_id (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_instances_add(cls_method_context_t hctx, bufferlist *in,
                         bufferlist *out) {
  std::string instance_id;
  try {
    auto iter = in->cbegin();
    decode(instance_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::instances_add(hctx, instance_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param instance_id (std::string)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_instances_remove(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string instance_id;
  try {
    auto iter = in->cbegin();
    decode(instance_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = mirror::instances_remove(hctx, instance_id);
  if (r < 0) {
    return r;
  }
  return 0;
}

/**
 * Input:
 * @param start_after: key to start after
 * @param max_return: max return items
 *
 * Output:
 * @param std::map<std::string, cls::rbd::MirrorImageMap>: image mapping
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_map_list(cls_method_context_t hctx, bufferlist *in,
                          bufferlist *out) {
  std::string start_after;
  uint64_t max_return;
  try {
    auto it = in->cbegin();
    decode(start_after, it);
    decode(max_return, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::map<std::string, cls::rbd::MirrorImageMap> image_mapping;
  int r = mirror::mirror_image_map_list(hctx, start_after, max_return, &image_mapping);
  if (r < 0) {
    return r;
  }

  encode(image_mapping, *out);
  return 0;
}

/**
 * Input:
 * @param global_image_id: global image id
 * @param image_map: image map
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_map_update(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string global_image_id;
  cls::rbd::MirrorImageMap image_map;

  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
    decode(image_map, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  bufferlist bl;
  encode(image_map, bl);

  const std::string key = mirror::mirror_image_map_key(global_image_id);
  int r = cls_cxx_map_set_val(hctx, key, &bl);
  if (r < 0) {
    CLS_ERR("error updating image map %s: %s", key.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/**
 * Input:
 * @param global_image_id: global image id
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int mirror_image_map_remove(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out) {
  std::string global_image_id;

  try {
    auto it = in->cbegin();
    decode(global_image_id, it);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  const std::string key = mirror::mirror_image_map_key(global_image_id);
  int r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error removing image map %s: %s", key.c_str(),
            cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

namespace group {

/********************** methods for rbd_group_directory ***********************/

int dir_add(cls_method_context_t hctx,
            const string &name, const string &id,
            bool check_for_unique_id)
{
  if (!name.size() || !is_valid_id(id)) {
    CLS_ERR("invalid group name '%s' or id '%s'",
            name.c_str(), id.c_str());
    return -EINVAL;
  }

  CLS_LOG(20, "dir_add name=%s id=%s", name.c_str(), id.c_str());

  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  string tmp;
  int r = read_key(hctx, name_key, &tmp);
  if (r != -ENOENT) {
    CLS_LOG(10, "name already exists");
    return -EEXIST;
  }
  r = read_key(hctx, id_key, &tmp);
  if (r != -ENOENT && check_for_unique_id) {
    CLS_LOG(10, "id already exists");
    return -EBADF;
  }
  bufferlist id_bl, name_bl;
  encode(id, id_bl);
  encode(name, name_bl);
  map<string, bufferlist> omap_vals;
  omap_vals[name_key] = id_bl;
  omap_vals[id_key] = name_bl;
  return cls_cxx_map_set_vals(hctx, &omap_vals);
}

int dir_remove(cls_method_context_t hctx,
               const string &name, const string &id)
{
  CLS_LOG(20, "dir_remove name=%s id=%s", name.c_str(), id.c_str());

  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  string stored_name, stored_id;

  int r = read_key(hctx, name_key, &stored_id);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading name to id mapping: %s", cpp_strerror(r).c_str());
    return r;
  }
  r = read_key(hctx, id_key, &stored_name);
  if (r < 0) {
    if (r != -ENOENT)
      CLS_ERR("error reading id to name mapping: %s", cpp_strerror(r).c_str());
    return r;
  }

  // check if this op raced with a rename
  if (stored_name != name || stored_id != id) {
    CLS_ERR("stored name '%s' and id '%s' do not match args '%s' and '%s'",
            stored_name.c_str(), stored_id.c_str(), name.c_str(), id.c_str());
    return -ESTALE;
  }

  r = cls_cxx_map_remove_key(hctx, name_key);
  if (r < 0) {
    CLS_ERR("error removing name: %s", cpp_strerror(r).c_str());
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, id_key);
  if (r < 0) {
    CLS_ERR("error removing id: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

static const string RBD_GROUP_SNAP_KEY_PREFIX = "snapshot_";

std::string snap_key(const std::string &snap_id) {
  ostringstream oss;
  oss << RBD_GROUP_SNAP_KEY_PREFIX << snap_id;
  return oss.str();
}

int snap_list(cls_method_context_t hctx, cls::rbd::GroupSnapshot start_after,
              uint64_t max_return,
              std::vector<cls::rbd::GroupSnapshot> *group_snaps)
{
  int max_read = RBD_MAX_KEYS_READ;
  std::map<string, bufferlist> vals;
  string last_read = snap_key(start_after.id);

  group_snaps->clear();

  bool more;
  do {
    int r = cls_cxx_map_get_vals(hctx, last_read,
				 RBD_GROUP_SNAP_KEY_PREFIX,
				 max_read, &vals, &more);
    if (r < 0)
      return r;

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end() && group_snaps->size() < max_return; ++it) {

      auto iter = it->second.cbegin();
      cls::rbd::GroupSnapshot snap;
      try {
	decode(snap, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("error decoding snapshot: %s", it->first.c_str());
	return -EIO;
      }
      CLS_LOG(20, "Discovered snapshot %s %s",
	      snap.name.c_str(),
	      snap.id.c_str());
      group_snaps->push_back(snap);
    }

  } while (more && (group_snaps->size() < max_return));

  return 0;
}

static int check_duplicate_snap_name(cls_method_context_t hctx,
				     const std::string &snap_name,
				     const std::string &snap_id)
{
  const int max_read = 1024;
  cls::rbd::GroupSnapshot snap_last;
  std::vector<cls::rbd::GroupSnapshot> page;

  for (;;) {
    int r = snap_list(hctx, snap_last, max_read, &page);
    if (r < 0) {
      return r;
    }
    for (auto& snap: page) {
      if (snap.name == snap_name && snap.id != snap_id) {
	return -EEXIST;
      }
    }

    if (page.size() < max_read) {
      break;
    }

    snap_last = *page.rbegin();
  }

  return 0;
}

} // namespace group

/**
 * List groups from the directory.
 *
 * Input:
 * @param start_after (std::string)
 * @param max_return (int64_t)
 *
 * Output:
 * @param map of groups (name, id)
 * @return 0 on success, negative error code on failure
 */
int group_dir_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  bool more = true;
  map<string, string> groups;
  string last_read = dir_key_for_name(start_after);

  while (more && groups.size() < max_return) {
    map<string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    int r = cls_cxx_map_get_vals(hctx, last_read, RBD_DIR_NAME_KEY_PREFIX,
                                 max_read, &vals, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("error reading directory by name: %s", cpp_strerror(r).c_str());
      }
      return r;
    }

    for (pair<string, bufferlist> val: vals) {
      string id;
      auto iter = val.second.cbegin();
      try {
	decode(id, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode id of group '%s'", val.first.c_str());
	return -EIO;
      }
      CLS_LOG(20, "adding '%s' -> '%s'", dir_name_from_key(val.first).c_str(), id.c_str());
      groups[dir_name_from_key(val.first)] = id;
      if (groups.size() >= max_return)
	break;
    }
    if (!vals.empty()) {
      last_read = dir_key_for_name(groups.rbegin()->first);
    }
  }

  encode(groups, *out);

  return 0;
}

/**
 * Add a group to the directory.
 *
 * Input:
 * @param name (std::string)
 * @param id (std::string)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_dir_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_cxx_create(hctx, false);

  if (r < 0) {
    CLS_ERR("could not create group directory: %s",
	    cpp_strerror(r).c_str());
    return r;
  }

  string name, id;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return group::dir_add(hctx, name, id, true);
}

/**
 * Rename a group to the directory.
 *
 * Input:
 * @param src original name of the group (std::string)
 * @param dest new name of the group (std::string)
 * @param id the id of the group (std::string)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_dir_rename(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string src, dest, id;
  try {
    auto iter = in->cbegin();
    decode(src, iter);
    decode(dest, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = group::dir_remove(hctx, src, id);
  if (r < 0)
    return r;

  return group::dir_add(hctx, dest, id, false);
}

/**
 * Remove a group from the directory.
 *
 * Input:
 * @param name (std::string)
 * @param id (std::string)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_dir_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name, id;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return group::dir_remove(hctx, name, id);
}

/**
 * Set state of an image in the group.
 *
 * Input:
 * @param image_status (cls::rbd::GroupImageStatus)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_image_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_image_set");

  cls::rbd::GroupImageStatus st;
  try {
    auto iter = in->cbegin();
    decode(st, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  string image_key = st.spec.image_key();

  bufferlist image_val_bl;
  encode(st.state, image_val_bl);
  int r = cls_cxx_map_set_val(hctx, image_key, &image_val_bl);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Remove reference to an image from the group.
 *
 * Input:
 * @param spec (cls::rbd::GroupImageSpec)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_image_remove(cls_method_context_t hctx,
                       bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_image_remove");
  cls::rbd::GroupImageSpec spec;
  try {
    auto iter = in->cbegin();
    decode(spec, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  string image_key = spec.image_key();

  int r = cls_cxx_map_remove_key(hctx, image_key);
  if (r < 0) {
    CLS_ERR("error removing image from group: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/*
 * List images in the group.
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param tuples of descriptions of the images: image_id, pool_id, image reference state.
 * @return 0 on success, negative error code on failure
 */
int group_image_list(cls_method_context_t hctx,
                     bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_image_list");
  cls::rbd::GroupImageSpec start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  std::map<string, bufferlist> vals;
  string last_read = start_after.image_key();
  std::vector<cls::rbd::GroupImageStatus> res;
  bool more;
  do {
    int r = cls_cxx_map_get_vals(hctx, last_read,
				 cls::rbd::RBD_GROUP_IMAGE_KEY_PREFIX,
				 max_read, &vals, &more);
    if (r < 0)
      return r;

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end() && res.size() < max_return; ++it) {

      auto iter = it->second.cbegin();
      cls::rbd::GroupImageLinkState state;
      try {
	decode(state, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("error decoding state for image: %s", it->first.c_str());
	return -EIO;
      }
      cls::rbd::GroupImageSpec spec;
      int r = cls::rbd::GroupImageSpec::from_key(it->first, &spec);
      if (r < 0)
	return r;

      CLS_LOG(20, "Discovered image %s %" PRId64 " %d", spec.image_id.c_str(),
	                                         spec.pool_id,
					         (int)state);
      res.push_back(cls::rbd::GroupImageStatus(spec, state));
    }
    if (res.size() > 0) {
      last_read = res.rbegin()->spec.image_key();
    }

  } while (more && (res.size() < max_return));
  encode(res, *out);

  return 0;
}

/**
 * Reference the group this image belongs to.
 *
 * Input:
 * @param group_id (std::string)
 * @param pool_id (int64_t)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int image_group_add(cls_method_context_t hctx,
		    bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "image_group_add");
  cls::rbd::GroupSpec new_group;
  try {
    auto iter = in->cbegin();
    decode(new_group, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  bufferlist existing_refbl;

  int r = cls_cxx_map_get_val(hctx, RBD_GROUP_REF, &existing_refbl);
  if (r == 0) {
    // If we are trying to link this image to the same group then return
    // success. If this image already belongs to another group then abort.
    cls::rbd::GroupSpec old_group;
    try {
      auto iter = existing_refbl.cbegin();
      decode(old_group, iter);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    if ((old_group.group_id != new_group.group_id) ||
        (old_group.pool_id != new_group.pool_id)) {
      return -EEXIST;
    } else {
      return 0; // In this case the values are already correct
    }
  } else if (r < 0 && r != -ENOENT) {
    // No entry means this image is not a member of any group.
    return r;
  }

  r = image::set_op_features(hctx, RBD_OPERATION_FEATURE_GROUP,
                             RBD_OPERATION_FEATURE_GROUP);
  if (r < 0) {
    return r;
  }

  bufferlist refbl;
  encode(new_group, refbl);
  r = cls_cxx_map_set_val(hctx, RBD_GROUP_REF, &refbl);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Remove image's pointer to the group.
 *
 * Input:
 * @param cg_id (std::string)
 * @param pool_id (int64_t)
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int image_group_remove(cls_method_context_t hctx,
		       bufferlist *in,
		       bufferlist *out)
{
  CLS_LOG(20, "image_group_remove");
  cls::rbd::GroupSpec spec;
  try {
    auto iter = in->cbegin();
    decode(spec, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  bufferlist refbl;
  int r = cls_cxx_map_get_val(hctx, RBD_GROUP_REF, &refbl);
  if (r < 0) {
    return r;
  }

  cls::rbd::GroupSpec ref_spec;
  auto iter = refbl.cbegin();
  try {
    decode(ref_spec, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (ref_spec.pool_id != spec.pool_id || ref_spec.group_id != spec.group_id) {
    return -EBADF;
  }

  r = cls_cxx_map_remove_key(hctx, RBD_GROUP_REF);
  if (r < 0) {
    return r;
  }

  r = image::set_op_features(hctx, 0, RBD_OPERATION_FEATURE_GROUP);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Retrieve the id and pool of the group this image belongs to.
 *
 * Input:
 * none
 *
 * Output:
 * @param GroupSpec
 * @return 0 on success, negative error code on failure
 */
int image_group_get(cls_method_context_t hctx,
		    bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "image_group_get");
  bufferlist refbl;
  int r = cls_cxx_map_get_val(hctx, RBD_GROUP_REF, &refbl);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  cls::rbd::GroupSpec spec;

  if (r != -ENOENT) {
    auto iter = refbl.cbegin();
    try {
      decode(spec, iter);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }
  }

  encode(spec, *out);
  return 0;
}

/**
 * Save initial snapshot record.
 *
 * Input:
 * @param GroupSnapshot
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_snap_set(cls_method_context_t hctx,
		   bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_snap_set");
  cls::rbd::GroupSnapshot group_snap;
  try {
    auto iter = in->cbegin();
    decode(group_snap, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (group_snap.name.empty()) {
    CLS_ERR("group snapshot name is empty");
    return -EINVAL;
  }
  if (group_snap.id.empty()) {
    CLS_ERR("group snapshot id is empty");
    return -EINVAL;
  }

  int r = group::check_duplicate_snap_name(hctx, group_snap.name,
                                           group_snap.id);
  if (r < 0) {
    return r;
  }

  std::string key = group::snap_key(group_snap.id);
  if (group_snap.state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
    bufferlist snap_bl;
    r = cls_cxx_map_get_val(hctx, key, &snap_bl);
    if (r < 0 && r != -ENOENT) {
      return r;
    } else if (r >= 0) {
      return -EEXIST;
    }
  }

  bufferlist obl;
  encode(group_snap, obl);
  r = cls_cxx_map_set_val(hctx, key, &obl);
  return r;
}

/**
 * Remove snapshot record.
 *
 * Input:
 * @param id Snapshot id
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int group_snap_remove(cls_method_context_t hctx,
		      bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_snap_remove");
  std::string snap_id;
  try {
    auto iter = in->cbegin();
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::string snap_key = group::snap_key(snap_id);

  CLS_LOG(20, "removing snapshot with key %s", snap_key.c_str());
  int r = cls_cxx_map_remove_key(hctx, snap_key);
  return r;
}

/**
 * Get group's snapshot by id.
 *
 * Input:
 * @param snapshot_id the id of the snapshot to look for.
 *
 * Output:
 * @param GroupSnapshot the requested snapshot
 * @return 0 on success, negative error code on failure
 */
int group_snap_get_by_id(cls_method_context_t hctx,
			 bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_snap_get_by_id");

  std::string snap_id;
  try {
    auto iter = in->cbegin();
    decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  bufferlist snapbl;

  int r = cls_cxx_map_get_val(hctx, group::snap_key(snap_id), &snapbl);
  if (r < 0) {
    return r;
  }

  cls::rbd::GroupSnapshot group_snap;
  auto iter = snapbl.cbegin();
  try {
    decode(group_snap, iter);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding snapshot: %s", snap_id.c_str());
    return -EIO;
  }

  encode(group_snap, *out);

  return 0;
}

/**
 * List group's snapshots.
 *
 * Input:
 * @param start_after which name to begin listing after
 * 	  (use the empty string to start at the beginning)
 * @param max_return the maximum number of snapshots to list
 *
 * Output:
 * @param list of snapshots
 * @return 0 on success, negative error code on failure
 */
int group_snap_list(cls_method_context_t hctx,
		    bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "group_snap_list");

  cls::rbd::GroupSnapshot start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  std::vector<cls::rbd::GroupSnapshot> group_snaps;
  group::snap_list(hctx, start_after, max_return, &group_snaps);

  encode(group_snaps, *out);

  return 0;
}

namespace trash {

static const std::string IMAGE_KEY_PREFIX("id_");

std::string image_key(const std::string &image_id) {
  return IMAGE_KEY_PREFIX + image_id;
}

std::string image_id_from_key(const std::string &key) {
  return key.substr(IMAGE_KEY_PREFIX.size());
}

} // namespace trash

/**
 * Add an image entry to the rbd trash. Creates the trash object if
 * needed, and stores the trash spec information of the deleted image.
 *
 * Input:
 * @param id the id of the image
 * @param trash_spec the spec info of the deleted image
 *
 * Output:
 * @returns -EEXIST if the image id is already in the trash
 * @returns 0 on success, negative error code on failure
 */
int trash_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_cxx_create(hctx, false);
  if (r < 0) {
    CLS_ERR("could not create trash: %s", cpp_strerror(r).c_str());
    return r;
  }

  string id;
  cls::rbd::TrashImageSpec trash_spec;
  try {
    auto iter = in->cbegin();
    decode(id, iter);
    decode(trash_spec, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (!is_valid_id(id)) {
    CLS_ERR("trash_add: invalid id '%s'", id.c_str());
    return -EINVAL;
  }

  CLS_LOG(20, "trash_add id=%s", id.c_str());

  string key = trash::image_key(id);
  cls::rbd::TrashImageSpec tmp;
  r = read_key(hctx, key, &tmp);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("could not read key %s entry from trash: %s", key.c_str(),
            cpp_strerror(r).c_str());
    return r;
  } else if (r == 0) {
    CLS_LOG(10, "id already exists");
    return -EEXIST;
  }

  map<string, bufferlist> omap_vals;
  encode(trash_spec, omap_vals[key]);
  return cls_cxx_map_set_vals(hctx, &omap_vals);
}

/**
 * Removes an image entry from the rbd trash object.
 * image.
 *
 * Input:
 * @param id the id of the image
 *
 * Output:
 * @returns -ENOENT if the image id does not exist in the trash
 * @returns 0 on success, negative error code on failure
 */
int trash_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;
  try {
    auto iter = in->cbegin();
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "trash_remove id=%s", id.c_str());

  string key = trash::image_key(id);
  bufferlist tmp;
  int r = cls_cxx_map_get_val(hctx, key, &tmp);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading entry key %s: %s", key.c_str(), cpp_strerror(r).c_str());
    }
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0) {
    CLS_ERR("error removing entry: %s", cpp_strerror(r).c_str());
    return r;
  }

  return 0;
}

/**
 * Returns the list of trash spec entries registered in the rbd_trash
 * object.
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param data the map between image id and trash spec info
 *
 * @returns 0 on success, negative error code on failure
 */
int trash_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  map<string, cls::rbd::TrashImageSpec> data;
  string last_read = trash::image_key(start_after);
  bool more = true;

  CLS_LOG(20, "trash_get_images");
  while (data.size() < max_return) {
    map<string, bufferlist> raw_data;
    int max_read = std::min<int32_t>(RBD_MAX_KEYS_READ,
                                     max_return - data.size());
    int r = cls_cxx_map_get_vals(hctx, last_read, trash::IMAGE_KEY_PREFIX,
                                 max_read, &raw_data, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("failed to read the vals off of disk: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }
    if (raw_data.empty()) {
      break;
    }

    map<string, bufferlist>::iterator it = raw_data.begin();
    for (; it != raw_data.end(); ++it) {
      decode(data[trash::image_id_from_key(it->first)], it->second);
    }

    if (!more) {
      break;
    }

    last_read = raw_data.rbegin()->first;
  }

  encode(data, *out);
  return 0;
}

/**
 * Returns the trash spec entry of an image registered in the rbd_trash
 * object.
 *
 * Input:
 * @param id the id of the image
 *
 * Output:
 * @param out the trash spec entry
 *
 * @returns 0 on success, negative error code on failure
 */
int trash_get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;
  try {
    auto iter = in->cbegin();
    decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "trash_get_image id=%s", id.c_str());


  string key = trash::image_key(id);
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, out);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("error reading image from trash '%s': '%s'", id.c_str(),
            cpp_strerror(r).c_str());
  }
  return r;
}

/**
 * Set state of an image in the rbd_trash object.
 *
 * Input:
 * @param id the id of the image
 * @param trash_state the state of the image to be set
 * @param expect_state the expected state of the image
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int trash_state_set(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;
  cls::rbd::TrashImageState trash_state;
  cls::rbd::TrashImageState expect_state;
  try {
    bufferlist::const_iterator iter = in->begin();
    decode(id, iter);
    decode(trash_state, iter);
    decode(expect_state, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "trash_state_set id=%s", id.c_str());

  string key = trash::image_key(id);
  cls::rbd::TrashImageSpec trash_spec;
  int r = read_key(hctx, key, &trash_spec);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("Could not read trash image spec off disk: %s",
              cpp_strerror(r).c_str());
    }
    return r;
  }

  if (trash_spec.state == expect_state) {
    trash_spec.state = trash_state;
    r = write_key(hctx, key, trash_spec);
    if (r < 0) {
      CLS_ERR("error setting trash image state: %s", cpp_strerror(r).c_str());
      return r;
    }

    return 0;
  } else if (trash_spec.state == trash_state) {
    return 0;
  } else {
    CLS_ERR("Current trash state: %d do not match expected: %d or set: %d",
            trash_spec.state, expect_state, trash_state);
    return -ESTALE;
  }
}

namespace nspace {

const std::string NAME_KEY_PREFIX("name_");

std::string key_for_name(const std::string& name) {
  return NAME_KEY_PREFIX + name;
}

std::string name_from_key(const std::string &key) {
  return key.substr(NAME_KEY_PREFIX.size());
}

} // namespace nspace

/**
 * Add a namespace to the namespace directory.
 *
 * Input:
 * @param name the name of the namespace
 *
 * Output:
 * @returns -EEXIST if the namespace is already exists
 * @returns 0 on success, negative error code on failure
 */
int namespace_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  std::string name;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::string key(nspace::key_for_name(name));
  bufferlist value;
  int r = cls_cxx_map_get_val(hctx, key, &value);
  if (r < 0 && r != -ENOENT) {
    return r;
  } else if (r == 0) {
    return -EEXIST;
  }

  r = cls_cxx_map_set_val(hctx, key, &value);
  if (r < 0) {
    CLS_ERR("failed to set omap key: %s", key.c_str());
    return r;
  }

  return 0;
}

/**
 * Remove a namespace from the namespace directory.
 *
 * Input:
 * @param name the name of the namespace
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int namespace_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  std::string name;
  try {
    auto iter = in->cbegin();
    decode(name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::string key(nspace::key_for_name(name));
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, key);
  if (r < 0) {
    return r;
  }

  return 0;
}

/**
 * Returns the list of namespaces in the rbd_namespace object
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param data list of namespace names
 * @returns 0 on success, negative error code on failure
 */
int namespace_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;
  try {
    auto iter = in->cbegin();
    decode(start_after, iter);
    decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  std::list<std::string> data;
  std::string last_read = nspace::key_for_name(start_after);
  bool more = true;

  CLS_LOG(20, "namespace_list");
  while (data.size() < max_return) {
    std::map<std::string, bufferlist> raw_data;
    int max_read = std::min<int32_t>(RBD_MAX_KEYS_READ,
                                     max_return - data.size());
    int r = cls_cxx_map_get_vals(hctx, last_read, nspace::NAME_KEY_PREFIX,
                                 max_read, &raw_data, &more);
    if (r < 0) {
      if (r != -ENOENT) {
        CLS_ERR("failed to read the vals off of disk: %s",
                cpp_strerror(r).c_str());
      }
      return r;
    }

    for (auto& it : raw_data) {
      data.push_back(nspace::name_from_key(it.first));
    }

    if (raw_data.empty() || !more) {
      break;
    }

    last_read = raw_data.rbegin()->first;
  }

  encode(data, *out);
  return 0;
}

/**
 *  Reclaim space for zeroed extents
 *
 * Input:
 * @param sparse_size minimal zeroed block to sparse
 * @param remove_empty boolean, true if the object should be removed if empty
 *
 * Output:
 * @returns -ENOENT if the object does not exist or has been removed
 * @returns 0 on success, negative error code on failure
 */
int sparsify(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  size_t sparse_size;
  bool remove_empty;
  try {
    auto iter = in->cbegin();
    decode(sparse_size, iter);
    decode(remove_empty, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0) {
    return r;
  }

  bufferlist bl;
  r = cls_cxx_read(hctx, 0, 0, &bl);
  if (r < 0) {
    CLS_ERR("failed to read data off of disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (bl.is_zero()) {
    if (remove_empty) {
      CLS_LOG(20, "remove");
      r = cls_cxx_remove(hctx);
      if (r < 0) {
        CLS_ERR("remove failed: %s", cpp_strerror(r).c_str());
        return r;
      }
    } else if (bl.length() > 0) {
      CLS_LOG(20, "truncate");
      bufferlist write_bl;
      r = cls_cxx_replace(hctx, 0, 0, &write_bl);
      if (r < 0) {
        CLS_ERR("truncate failed: %s", cpp_strerror(r).c_str());
        return r;
      }
    } else {
      CLS_LOG(20, "skip empty");
    }
    return 0;
  }

  bl.rebuild(buffer::ptr_node::create(bl.length()));
  size_t write_offset = 0;
  size_t write_length = 0;
  size_t offset = 0;
  size_t length = bl.length();
  const auto& ptr = bl.front();
  bool replace = true;
  while (offset < length) {
    if (calc_sparse_extent(ptr, sparse_size, length, &write_offset,
                           &write_length, &offset)) {
      if (write_offset == 0 && write_length == length) {
        CLS_LOG(20, "nothing to do");
        return 0;
      }
      CLS_LOG(20, "write%s %" PRIu64 "~%" PRIu64, (replace ? "(replace)" : ""),
              write_offset, write_length);
      bufferlist write_bl;
      write_bl.push_back(buffer::ptr_node::create(ptr, write_offset,
                                                  write_length));
      if (replace) {
        r = cls_cxx_replace(hctx, write_offset, write_length, &write_bl);
        replace = false;
      } else {
        r = cls_cxx_write(hctx, write_offset, write_length, &write_bl);
      }
      if (r < 0) {
        CLS_ERR("write failed: %s", cpp_strerror(r).c_str());
        return r;
      }
      write_offset = offset;
      write_length = 0;
    }
  }

  return 0;
}

CLS_INIT(rbd)
{
  CLS_LOG(20, "Loaded rbd class!");

  cls_handle_t h_class;
  cls_method_handle_t h_create;
  cls_method_handle_t h_get_features;
  cls_method_handle_t h_set_features;
  cls_method_handle_t h_get_size;
  cls_method_handle_t h_set_size;
  cls_method_handle_t h_get_parent;
  cls_method_handle_t h_set_parent;
  cls_method_handle_t h_remove_parent;
  cls_method_handle_t h_parent_get;
  cls_method_handle_t h_parent_overlap_get;
  cls_method_handle_t h_parent_attach;
  cls_method_handle_t h_parent_detach;
  cls_method_handle_t h_get_protection_status;
  cls_method_handle_t h_set_protection_status;
  cls_method_handle_t h_get_stripe_unit_count;
  cls_method_handle_t h_set_stripe_unit_count;
  cls_method_handle_t h_get_create_timestamp;
  cls_method_handle_t h_get_access_timestamp;
  cls_method_handle_t h_get_modify_timestamp;
  cls_method_handle_t h_get_flags;
  cls_method_handle_t h_set_flags;
  cls_method_handle_t h_op_features_get;
  cls_method_handle_t h_op_features_set;
  cls_method_handle_t h_add_child;
  cls_method_handle_t h_remove_child;
  cls_method_handle_t h_get_children;
  cls_method_handle_t h_get_snapcontext;
  cls_method_handle_t h_get_object_prefix;
  cls_method_handle_t h_get_data_pool;
  cls_method_handle_t h_get_snapshot_name;
  cls_method_handle_t h_get_snapshot_timestamp;
  cls_method_handle_t h_snapshot_get;
  cls_method_handle_t h_snapshot_add;
  cls_method_handle_t h_snapshot_remove;
  cls_method_handle_t h_snapshot_rename;
  cls_method_handle_t h_snapshot_trash_add;
  cls_method_handle_t h_get_all_features;
  cls_method_handle_t h_get_id;
  cls_method_handle_t h_set_id;
  cls_method_handle_t h_set_modify_timestamp;
  cls_method_handle_t h_set_access_timestamp;
  cls_method_handle_t h_dir_get_id;
  cls_method_handle_t h_dir_get_name;
  cls_method_handle_t h_dir_list;
  cls_method_handle_t h_dir_add_image;
  cls_method_handle_t h_dir_remove_image;
  cls_method_handle_t h_dir_rename_image;
  cls_method_handle_t h_dir_state_assert;
  cls_method_handle_t h_dir_state_set;
  cls_method_handle_t h_object_map_load;
  cls_method_handle_t h_object_map_save;
  cls_method_handle_t h_object_map_resize;
  cls_method_handle_t h_object_map_update;
  cls_method_handle_t h_object_map_snap_add;
  cls_method_handle_t h_object_map_snap_remove;
  cls_method_handle_t h_metadata_set;
  cls_method_handle_t h_metadata_remove;
  cls_method_handle_t h_metadata_list;
  cls_method_handle_t h_metadata_get;
  cls_method_handle_t h_snapshot_get_limit;
  cls_method_handle_t h_snapshot_set_limit;
  cls_method_handle_t h_child_attach;
  cls_method_handle_t h_child_detach;
  cls_method_handle_t h_children_list;
  cls_method_handle_t h_migration_set;
  cls_method_handle_t h_migration_set_state;
  cls_method_handle_t h_migration_get;
  cls_method_handle_t h_migration_remove;
  cls_method_handle_t h_old_snapshots_list;
  cls_method_handle_t h_old_snapshot_add;
  cls_method_handle_t h_old_snapshot_remove;
  cls_method_handle_t h_old_snapshot_rename;
  cls_method_handle_t h_mirror_uuid_get;
  cls_method_handle_t h_mirror_uuid_set;
  cls_method_handle_t h_mirror_mode_get;
  cls_method_handle_t h_mirror_mode_set;
  cls_method_handle_t h_mirror_peer_list;
  cls_method_handle_t h_mirror_peer_add;
  cls_method_handle_t h_mirror_peer_remove;
  cls_method_handle_t h_mirror_peer_set_client;
  cls_method_handle_t h_mirror_peer_set_cluster;
  cls_method_handle_t h_mirror_image_list;
  cls_method_handle_t h_mirror_image_get_image_id;
  cls_method_handle_t h_mirror_image_get;
  cls_method_handle_t h_mirror_image_set;
  cls_method_handle_t h_mirror_image_remove;
  cls_method_handle_t h_mirror_image_status_set;
  cls_method_handle_t h_mirror_image_status_remove;
  cls_method_handle_t h_mirror_image_status_get;
  cls_method_handle_t h_mirror_image_status_list;
  cls_method_handle_t h_mirror_image_status_get_summary;
  cls_method_handle_t h_mirror_image_status_remove_down;
  cls_method_handle_t h_mirror_image_instance_get;
  cls_method_handle_t h_mirror_image_instance_list;
  cls_method_handle_t h_mirror_instances_list;
  cls_method_handle_t h_mirror_instances_add;
  cls_method_handle_t h_mirror_instances_remove;
  cls_method_handle_t h_mirror_image_map_list;
  cls_method_handle_t h_mirror_image_map_update;
  cls_method_handle_t h_mirror_image_map_remove;
  cls_method_handle_t h_group_dir_list;
  cls_method_handle_t h_group_dir_add;
  cls_method_handle_t h_group_dir_remove;
  cls_method_handle_t h_group_dir_rename;
  cls_method_handle_t h_group_image_remove;
  cls_method_handle_t h_group_image_list;
  cls_method_handle_t h_group_image_set;
  cls_method_handle_t h_image_group_add;
  cls_method_handle_t h_image_group_remove;
  cls_method_handle_t h_image_group_get;
  cls_method_handle_t h_group_snap_set;
  cls_method_handle_t h_group_snap_remove;
  cls_method_handle_t h_group_snap_get_by_id;
  cls_method_handle_t h_group_snap_list;
  cls_method_handle_t h_trash_add;
  cls_method_handle_t h_trash_remove;
  cls_method_handle_t h_trash_list;
  cls_method_handle_t h_trash_get;
  cls_method_handle_t h_trash_state_set;
  cls_method_handle_t h_namespace_add;
  cls_method_handle_t h_namespace_remove;
  cls_method_handle_t h_namespace_list;
  cls_method_handle_t h_copyup;
  cls_method_handle_t h_assert_snapc_seq;
  cls_method_handle_t h_sparsify;

  cls_register("rbd", &h_class);
  cls_register_cxx_method(h_class, "create",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  create, &h_create);
  cls_register_cxx_method(h_class, "get_features",
			  CLS_METHOD_RD,
			  get_features, &h_get_features);
  cls_register_cxx_method(h_class, "set_features",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_features, &h_set_features);
  cls_register_cxx_method(h_class, "get_size",
			  CLS_METHOD_RD,
			  get_size, &h_get_size);
  cls_register_cxx_method(h_class, "set_size",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_size, &h_set_size);
  cls_register_cxx_method(h_class, "get_snapcontext",
			  CLS_METHOD_RD,
			  get_snapcontext, &h_get_snapcontext);
  cls_register_cxx_method(h_class, "get_object_prefix",
			  CLS_METHOD_RD,
			  get_object_prefix, &h_get_object_prefix);
  cls_register_cxx_method(h_class, "get_data_pool", CLS_METHOD_RD,
                          get_data_pool, &h_get_data_pool);
  cls_register_cxx_method(h_class, "get_snapshot_name",
			  CLS_METHOD_RD,
			  get_snapshot_name, &h_get_snapshot_name);
  cls_register_cxx_method(h_class, "get_snapshot_timestamp",
			  CLS_METHOD_RD,
			  get_snapshot_timestamp, &h_get_snapshot_timestamp);
  cls_register_cxx_method(h_class, "snapshot_get",
                          CLS_METHOD_RD,
                          snapshot_get, &h_snapshot_get);
  cls_register_cxx_method(h_class, "snapshot_add",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  snapshot_add, &h_snapshot_add);
  cls_register_cxx_method(h_class, "snapshot_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  snapshot_remove, &h_snapshot_remove);
  cls_register_cxx_method(h_class, "snapshot_rename",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  snapshot_rename, &h_snapshot_rename);
  cls_register_cxx_method(h_class, "snapshot_trash_add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          snapshot_trash_add, &h_snapshot_trash_add);
  cls_register_cxx_method(h_class, "get_all_features",
			  CLS_METHOD_RD,
			  get_all_features, &h_get_all_features);

  // NOTE: deprecate v1 parent APIs after mimic EOLed
  cls_register_cxx_method(h_class, "get_parent",
			  CLS_METHOD_RD,
			  get_parent, &h_get_parent);
  cls_register_cxx_method(h_class, "set_parent",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_parent, &h_set_parent);
  cls_register_cxx_method(h_class, "remove_parent",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  remove_parent, &h_remove_parent);

  cls_register_cxx_method(h_class, "parent_get",
                          CLS_METHOD_RD, parent_get, &h_parent_get);
  cls_register_cxx_method(h_class, "parent_overlap_get",
                          CLS_METHOD_RD, parent_overlap_get,
                          &h_parent_overlap_get);
  cls_register_cxx_method(h_class, "parent_attach",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          parent_attach, &h_parent_attach);
  cls_register_cxx_method(h_class, "parent_detach",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          parent_detach, &h_parent_detach);

  cls_register_cxx_method(h_class, "set_protection_status",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_protection_status, &h_set_protection_status);
  cls_register_cxx_method(h_class, "get_protection_status",
			  CLS_METHOD_RD,
			  get_protection_status, &h_get_protection_status);
  cls_register_cxx_method(h_class, "get_stripe_unit_count",
			  CLS_METHOD_RD,
			  get_stripe_unit_count, &h_get_stripe_unit_count);
  cls_register_cxx_method(h_class, "set_stripe_unit_count",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_stripe_unit_count, &h_set_stripe_unit_count);
  cls_register_cxx_method(h_class, "get_create_timestamp",
                          CLS_METHOD_RD,
                          get_create_timestamp, &h_get_create_timestamp);
  cls_register_cxx_method(h_class, "get_access_timestamp",
                          CLS_METHOD_RD,
                          get_access_timestamp, &h_get_access_timestamp);
  cls_register_cxx_method(h_class, "get_modify_timestamp",
                          CLS_METHOD_RD,
                          get_modify_timestamp, &h_get_modify_timestamp);
  cls_register_cxx_method(h_class, "get_flags",
                          CLS_METHOD_RD,
                          get_flags, &h_get_flags);
  cls_register_cxx_method(h_class, "set_flags",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          set_flags, &h_set_flags);
  cls_register_cxx_method(h_class, "op_features_get", CLS_METHOD_RD,
                          op_features_get, &h_op_features_get);
  cls_register_cxx_method(h_class, "op_features_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          op_features_set, &h_op_features_set);
  cls_register_cxx_method(h_class, "metadata_list",
                          CLS_METHOD_RD,
			  metadata_list, &h_metadata_list);
  cls_register_cxx_method(h_class, "metadata_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  metadata_set, &h_metadata_set);
  cls_register_cxx_method(h_class, "metadata_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  metadata_remove, &h_metadata_remove);
  cls_register_cxx_method(h_class, "metadata_get",
                          CLS_METHOD_RD,
			  metadata_get, &h_metadata_get);
  cls_register_cxx_method(h_class, "snapshot_get_limit",
			  CLS_METHOD_RD,
			  snapshot_get_limit, &h_snapshot_get_limit);
  cls_register_cxx_method(h_class, "snapshot_set_limit",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  snapshot_set_limit, &h_snapshot_set_limit);
  cls_register_cxx_method(h_class, "child_attach",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          child_attach, &h_child_attach);
  cls_register_cxx_method(h_class, "child_detach",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          child_detach, &h_child_detach);
  cls_register_cxx_method(h_class, "children_list",
                          CLS_METHOD_RD,
                          children_list, &h_children_list);
  cls_register_cxx_method(h_class, "migration_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          migration_set, &h_migration_set);
  cls_register_cxx_method(h_class, "migration_set_state",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          migration_set_state, &h_migration_set_state);
  cls_register_cxx_method(h_class, "migration_get",
                          CLS_METHOD_RD,
                          migration_get, &h_migration_get);
  cls_register_cxx_method(h_class, "migration_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          migration_remove, &h_migration_remove);

  cls_register_cxx_method(h_class, "set_modify_timestamp",
	            	  CLS_METHOD_RD | CLS_METHOD_WR,
                          set_modify_timestamp, &h_set_modify_timestamp);

  cls_register_cxx_method(h_class, "set_access_timestamp",
	            	  CLS_METHOD_RD | CLS_METHOD_WR,
                          set_access_timestamp, &h_set_access_timestamp);

  /* methods for the rbd_children object */
  cls_register_cxx_method(h_class, "add_child",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  add_child, &h_add_child);
  cls_register_cxx_method(h_class, "remove_child",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  remove_child, &h_remove_child);
  cls_register_cxx_method(h_class, "get_children",
			  CLS_METHOD_RD,
			  get_children, &h_get_children);

  /* methods for the rbd_id.$image_name objects */
  cls_register_cxx_method(h_class, "get_id",
			  CLS_METHOD_RD,
			  get_id, &h_get_id);
  cls_register_cxx_method(h_class, "set_id",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  set_id, &h_set_id);

  /* methods for the rbd_directory object */
  cls_register_cxx_method(h_class, "dir_get_id",
			  CLS_METHOD_RD,
			  dir_get_id, &h_dir_get_id);
  cls_register_cxx_method(h_class, "dir_get_name",
			  CLS_METHOD_RD,
			  dir_get_name, &h_dir_get_name);
  cls_register_cxx_method(h_class, "dir_list",
			  CLS_METHOD_RD,
			  dir_list, &h_dir_list);
  cls_register_cxx_method(h_class, "dir_add_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_add_image, &h_dir_add_image);
  cls_register_cxx_method(h_class, "dir_remove_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_remove_image, &h_dir_remove_image);
  cls_register_cxx_method(h_class, "dir_rename_image",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  dir_rename_image, &h_dir_rename_image);
  cls_register_cxx_method(h_class, "dir_state_assert", CLS_METHOD_RD,
                          dir_state_assert, &h_dir_state_assert);
  cls_register_cxx_method(h_class, "dir_state_set",
			  CLS_METHOD_RD | CLS_METHOD_WR,
                          dir_state_set, &h_dir_state_set);

  /* methods for the rbd_object_map.$image_id object */
  cls_register_cxx_method(h_class, "object_map_load",
                          CLS_METHOD_RD,
			  object_map_load, &h_object_map_load);
  cls_register_cxx_method(h_class, "object_map_save",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  object_map_save, &h_object_map_save);
  cls_register_cxx_method(h_class, "object_map_resize",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  object_map_resize, &h_object_map_resize);
  cls_register_cxx_method(h_class, "object_map_update",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  object_map_update, &h_object_map_update);
  cls_register_cxx_method(h_class, "object_map_snap_add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  object_map_snap_add, &h_object_map_snap_add);
  cls_register_cxx_method(h_class, "object_map_snap_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
			  object_map_snap_remove, &h_object_map_snap_remove);

 /* methods for the old format */
  cls_register_cxx_method(h_class, "snap_list",
			  CLS_METHOD_RD,
			  old_snapshots_list, &h_old_snapshots_list);
  cls_register_cxx_method(h_class, "snap_add",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  old_snapshot_add, &h_old_snapshot_add);
  cls_register_cxx_method(h_class, "snap_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  old_snapshot_remove, &h_old_snapshot_remove);
  cls_register_cxx_method(h_class, "snap_rename",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  old_snapshot_rename, &h_old_snapshot_rename);

  /* methods for the rbd_mirroring object */
  cls_register_cxx_method(h_class, "mirror_uuid_get", CLS_METHOD_RD,
                          mirror_uuid_get, &h_mirror_uuid_get);
  cls_register_cxx_method(h_class, "mirror_uuid_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_uuid_set, &h_mirror_uuid_set);
  cls_register_cxx_method(h_class, "mirror_mode_get", CLS_METHOD_RD,
                          mirror_mode_get, &h_mirror_mode_get);
  cls_register_cxx_method(h_class, "mirror_mode_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_mode_set, &h_mirror_mode_set);
  cls_register_cxx_method(h_class, "mirror_peer_list", CLS_METHOD_RD,
                          mirror_peer_list, &h_mirror_peer_list);
  cls_register_cxx_method(h_class, "mirror_peer_add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_peer_add, &h_mirror_peer_add);
  cls_register_cxx_method(h_class, "mirror_peer_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_peer_remove, &h_mirror_peer_remove);
  cls_register_cxx_method(h_class, "mirror_peer_set_client",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_peer_set_client, &h_mirror_peer_set_client);
  cls_register_cxx_method(h_class, "mirror_peer_set_cluster",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_peer_set_cluster, &h_mirror_peer_set_cluster);
  cls_register_cxx_method(h_class, "mirror_image_list", CLS_METHOD_RD,
                          mirror_image_list, &h_mirror_image_list);
  cls_register_cxx_method(h_class, "mirror_image_get_image_id", CLS_METHOD_RD,
                          mirror_image_get_image_id,
                          &h_mirror_image_get_image_id);
  cls_register_cxx_method(h_class, "mirror_image_get", CLS_METHOD_RD,
                          mirror_image_get, &h_mirror_image_get);
  cls_register_cxx_method(h_class, "mirror_image_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_image_set, &h_mirror_image_set);
  cls_register_cxx_method(h_class, "mirror_image_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_image_remove, &h_mirror_image_remove);
  cls_register_cxx_method(h_class, "mirror_image_status_set",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE,
                          mirror_image_status_set, &h_mirror_image_status_set);
  cls_register_cxx_method(h_class, "mirror_image_status_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_image_status_remove,
			  &h_mirror_image_status_remove);
  cls_register_cxx_method(h_class, "mirror_image_status_get", CLS_METHOD_RD,
                          mirror_image_status_get, &h_mirror_image_status_get);
  cls_register_cxx_method(h_class, "mirror_image_status_list", CLS_METHOD_RD,
                          mirror_image_status_list,
			  &h_mirror_image_status_list);
  cls_register_cxx_method(h_class, "mirror_image_status_get_summary",
			  CLS_METHOD_RD, mirror_image_status_get_summary,
			  &h_mirror_image_status_get_summary);
  cls_register_cxx_method(h_class, "mirror_image_status_remove_down",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_image_status_remove_down,
			  &h_mirror_image_status_remove_down);
  cls_register_cxx_method(h_class, "mirror_image_instance_get", CLS_METHOD_RD,
                          mirror_image_instance_get,
                          &h_mirror_image_instance_get);
  cls_register_cxx_method(h_class, "mirror_image_instance_list", CLS_METHOD_RD,
                          mirror_image_instance_list,
                          &h_mirror_image_instance_list);
  cls_register_cxx_method(h_class, "mirror_instances_list", CLS_METHOD_RD,
                          mirror_instances_list, &h_mirror_instances_list);
  cls_register_cxx_method(h_class, "mirror_instances_add",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE,
                          mirror_instances_add, &h_mirror_instances_add);
  cls_register_cxx_method(h_class, "mirror_instances_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          mirror_instances_remove,
                          &h_mirror_instances_remove);
  cls_register_cxx_method(h_class, "mirror_image_map_list",
                          CLS_METHOD_RD, mirror_image_map_list,
                          &h_mirror_image_map_list);
  cls_register_cxx_method(h_class, "mirror_image_map_update",
                          CLS_METHOD_WR, mirror_image_map_update,
                          &h_mirror_image_map_update);
  cls_register_cxx_method(h_class, "mirror_image_map_remove",
                          CLS_METHOD_WR, mirror_image_map_remove,
                          &h_mirror_image_map_remove);

  /* methods for the groups feature */
  cls_register_cxx_method(h_class, "group_dir_list",
			  CLS_METHOD_RD,
			  group_dir_list, &h_group_dir_list);
  cls_register_cxx_method(h_class, "group_dir_add",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_dir_add, &h_group_dir_add);
  cls_register_cxx_method(h_class, "group_dir_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_dir_remove, &h_group_dir_remove);
  cls_register_cxx_method(h_class, "group_dir_rename",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          group_dir_rename, &h_group_dir_rename);
  cls_register_cxx_method(h_class, "group_image_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_image_remove, &h_group_image_remove);
  cls_register_cxx_method(h_class, "group_image_list",
			  CLS_METHOD_RD,
			  group_image_list, &h_group_image_list);
  cls_register_cxx_method(h_class, "group_image_set",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_image_set, &h_group_image_set);
  cls_register_cxx_method(h_class, "image_group_add",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  image_group_add, &h_image_group_add);
  cls_register_cxx_method(h_class, "image_group_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  image_group_remove, &h_image_group_remove);
  cls_register_cxx_method(h_class, "image_group_get",
			  CLS_METHOD_RD,
			  image_group_get, &h_image_group_get);
  cls_register_cxx_method(h_class, "group_snap_set",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_snap_set, &h_group_snap_set);
  cls_register_cxx_method(h_class, "group_snap_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  group_snap_remove, &h_group_snap_remove);
  cls_register_cxx_method(h_class, "group_snap_get_by_id",
			  CLS_METHOD_RD,
			  group_snap_get_by_id, &h_group_snap_get_by_id);
  cls_register_cxx_method(h_class, "group_snap_list",
			  CLS_METHOD_RD,
			  group_snap_list, &h_group_snap_list);

  /* rbd_trash object methods */
  cls_register_cxx_method(h_class, "trash_add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          trash_add, &h_trash_add);
  cls_register_cxx_method(h_class, "trash_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          trash_remove, &h_trash_remove);
  cls_register_cxx_method(h_class, "trash_list",
                          CLS_METHOD_RD,
                          trash_list, &h_trash_list);
  cls_register_cxx_method(h_class, "trash_get",
                          CLS_METHOD_RD,
                          trash_get, &h_trash_get);
  cls_register_cxx_method(h_class, "trash_state_set",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          trash_state_set, &h_trash_state_set);

  /* rbd_namespace object methods */
  cls_register_cxx_method(h_class, "namespace_add",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          namespace_add, &h_namespace_add);
  cls_register_cxx_method(h_class, "namespace_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          namespace_remove, &h_namespace_remove);
  cls_register_cxx_method(h_class, "namespace_list", CLS_METHOD_RD,
                          namespace_list, &h_namespace_list);

  /* data object methods */
  cls_register_cxx_method(h_class, "copyup",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  copyup, &h_copyup);
  cls_register_cxx_method(h_class, "assert_snapc_seq",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          assert_snapc_seq,
                          &h_assert_snapc_seq);
  cls_register_cxx_method(h_class, "sparsify",
			  CLS_METHOD_RD | CLS_METHOD_WR,
			  sparsify, &h_sparsify);
}
