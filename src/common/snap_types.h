#ifndef __CEPH_SNAP_TYPES_H
#define __CEPH_SNAP_TYPES_H

#include "include/types.h"
#include "include/fs_types.h"

namespace ceph {

class Formatter;
}
struct SnapRealmInfo {
  mutable ceph_mds_snap_realm h;
  vector<snapid_t> my_snaps;
  vector<snapid_t> prior_parent_snaps;  // before parent_since

  SnapRealmInfo() {
    memset(&h, 0, sizeof(h));
  }
  SnapRealmInfo(inodeno_t ino_, snapid_t created_, snapid_t seq_, snapid_t current_parent_since_) {
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

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapRealmInfo*>& o);
};
WRITE_CLASS_ENCODER(SnapRealmInfo)


struct SnapContext {
  snapid_t seq;            // 'time' stamp
  vector<snapid_t> snaps;  // existent snaps, in descending order

  SnapContext() {}
  SnapContext(snapid_t s, const vector<snapid_t>& v) : seq(s), snaps(v) {}    

  bool is_valid() const;

  void clear() {
    seq = 0;
    snaps.clear();
  }
  bool empty() { return seq == 0; }

  void encode(bufferlist& bl) const {
    ::encode(seq, bl);
    ::encode(snaps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(seq, bl);
    ::decode(snaps, bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapContext*>& o);
};
WRITE_CLASS_ENCODER(SnapContext)

inline ostream& operator<<(ostream& out, const SnapContext& snapc) {
  return out << snapc.seq << "=" << snapc.snaps;
}

//}

#endif
