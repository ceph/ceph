#ifndef __CEPH_SNAP_TYPES_H
#define __CEPH_SNAP_TYPES_H

#include "include/object.h" // for struct snapid_t
#include "include/utime.h"
#include "include/fs_types.h" // for struct inodeno_t

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

namespace ceph {
class Formatter;
}

struct SnapRealmInfo {
  mutable ceph_mds_snap_realm h;
  std::vector<snapid_t> my_snaps;
  std::vector<snapid_t> prior_parent_snaps;  // before parent_since

  SnapRealmInfo() {
    // FIPS zeroization audit 20191115: this memset is not security related.
    memset(&h, 0, sizeof(h));
  }
  SnapRealmInfo(inodeno_t ino_, snapid_t created_, snapid_t seq_, snapid_t current_parent_since_) {
    // FIPS zeroization audit 20191115: this memset is not security related.
    memset(&h, 0, sizeof(h));
    h.ino = ino_;
    h.created = created_;
    h.seq = seq_;
    h.parent_since = current_parent_since_;
  }

  inodeno_t ino() const { return inodeno_t(h.ino); }
  inodeno_t parent() const { return inodeno_t(h.parent); }
  snapid_t seq() const { return snapid_t(h.seq); }
  snapid_t parent_since() const { return snapid_t(h.parent_since); }
  snapid_t created() const { return snapid_t(h.created); }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<SnapRealmInfo> generate_test_instances();
};
WRITE_CLASS_ENCODER(SnapRealmInfo)

// "new* snap realm info - carries additional metadata (last modified,
// change_attr) and is version encoded.
struct SnapRealmInfoNew {
  SnapRealmInfo info;
  utime_t last_modified;
  uint64_t change_attr;

  SnapRealmInfoNew() {
  }

  SnapRealmInfoNew(const SnapRealmInfo &info_, utime_t last_modified_, uint64_t change_attr_) {
    // FIPS zeroization audit 20191115: this memset is not security related.
    info = info_;
    last_modified = last_modified_;
    change_attr = change_attr_;
  }

  inodeno_t ino() const { return inodeno_t(info.h.ino); }
  inodeno_t parent() const { return inodeno_t(info.h.parent); }
  snapid_t seq() const { return snapid_t(info.h.seq); }
  snapid_t parent_since() const { return snapid_t(info.h.parent_since); }
  snapid_t created() const { return snapid_t(info.h.created); }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<SnapRealmInfoNew> generate_test_instances();
};
WRITE_CLASS_ENCODER(SnapRealmInfoNew)

struct SnapContext {
  snapid_t seq;            // 'time' stamp
  std::vector<snapid_t> snaps;  // existent snaps, in descending order

  SnapContext() {}
  SnapContext(snapid_t s, const std::vector<snapid_t>& v) : seq(s), snaps(v) {}    

  bool is_valid() const;

  void clear() {
    seq = 0;
    snaps.clear();
  }
  bool empty() const { return seq == 0; }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<SnapContext> generate_test_instances();
};
WRITE_CLASS_ENCODER(SnapContext)

std::ostream& operator<<(std::ostream& out, const SnapContext& snapc);

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<SnapContext> : fmt::ostream_formatter {};
#endif

#endif
