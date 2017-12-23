// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_TYPES_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_TYPES_H

#include <map>
#include <string>
#include <boost/variant.hpp>

#include "include/buffer.h"
#include "include/encoding.h"
#include "tools/rbd_mirror/types.h"

namespace ceph {
class Formatter;
}

namespace rbd {
namespace mirror {
namespace image_map {

struct Listener {
  virtual ~Listener() {
  }

  virtual void acquire_image(const std::string &global_image_id,
                             const std::string &instance_id) = 0;
  virtual void release_image(const std::string &global_image_id,
                             const std::string &instance_id) = 0;
  virtual void remove_image(const std::string &mirror_uuid,
                            const std::string &global_image_id,
                            const std::string &instance_id) = 0;
};

enum PolicyMetaType {
  POLICY_META_TYPE_NONE = 0,
};

struct PolicyMetaNone {
  static const PolicyMetaType TYPE = POLICY_META_TYPE_NONE;

  PolicyMetaNone() {
  }

  void encode(bufferlist& bl) const {
  }

  void decode(__u8 version, bufferlist::iterator& it) {
  }

  void dump(Formatter *f) const {
  }
};

struct PolicyMetaUnknown {
  static const PolicyMetaType TYPE = static_cast<PolicyMetaType>(-1);

  PolicyMetaUnknown() {
  }

  void encode(bufferlist& bl) const {
    assert(false);
  }

  void decode(__u8 version, bufferlist::iterator& it) {
  }

  void dump(Formatter *f) const {
  }
};

typedef boost::variant<PolicyMetaNone,
                       PolicyMetaUnknown> PolicyMeta;

struct PolicyData {
  PolicyData()
    : policy_meta(PolicyMetaUnknown()) {
  }
  PolicyData(const PolicyMeta &policy_meta)
    : policy_meta(policy_meta) {
  }

  PolicyMeta policy_meta;

  PolicyMetaType get_policy_meta_type() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<PolicyData *> &o);
};

WRITE_CLASS_ENCODER(PolicyData);

} // namespace image_map
} // namespace mirror
} // namespace rbd


#endif // CEPH_RBD_MIRROR_IMAGE_MAP_TYPES_H
