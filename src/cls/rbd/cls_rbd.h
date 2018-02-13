// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_CLS_RBD_H
#define __CEPH_CLS_RBD_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "include/rbd_types.h"
#include "common/Formatter.h"
#include "cls/rbd/cls_rbd_types.h"

/// information about our parent image, if any
struct cls_rbd_parent {
  int64_t pool;        ///< parent pool id
  string id;           ///< parent image id
  snapid_t snapid;     ///< parent snapid we refer to
  uint64_t overlap;    ///< portion of this image mapped onto parent (bytes)

  /// true if our parent pointer information is defined
  bool exists() const {
    return snapid != CEPH_NOSNAP && pool >= 0 && id.length() > 0 && overlap > 0;
  }

  cls_rbd_parent() : pool(-1), snapid(CEPH_NOSNAP), overlap(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(pool, bl);
    encode(id, bl);
    encode(snapid, bl);
    encode(overlap, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(pool, bl);
    decode(id, bl);
    decode(snapid, bl);
    decode(overlap, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_int("pool", pool);
    f->dump_string("id", id);
    f->dump_unsigned("snapid", snapid);
    f->dump_unsigned("overlap", overlap);
  }
  static void generate_test_instances(list<cls_rbd_parent*>& o) {
    o.push_back(new cls_rbd_parent);
    cls_rbd_parent *t = new cls_rbd_parent;
    t->pool = 1;
    t->id = "foo";
    t->snapid = 3;
    t->overlap = 500;
    o.push_back(t);
  }
};
WRITE_CLASS_ENCODER(cls_rbd_parent)

struct cls_rbd_snap {
  snapid_t id = CEPH_NOSNAP;
  string name;
  uint64_t image_size = 0;
  uint8_t protection_status = RBD_PROTECTION_STATUS_UNPROTECTED;
  cls_rbd_parent parent;
  uint64_t flags = 0;
  utime_t timestamp;
  cls::rbd::SnapshotNamespace snapshot_namespace = {
    cls::rbd::UserSnapshotNamespace{}};
  uint32_t child_count = 0;

  /// true if we have a parent
  bool has_parent() const {
    return parent.exists();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(7, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(image_size, bl);
    uint64_t features = 0;
    encode(features, bl); // unused -- preserve ABI
    encode(parent, bl);
    encode(protection_status, bl);
    encode(flags, bl);
    encode(snapshot_namespace, bl);
    encode(timestamp, bl);
    encode(child_count, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) {
    DECODE_START(7, p);
    decode(id, p);
    decode(name, p);
    decode(image_size, p);
    uint64_t features;
    decode(features, p); // unused -- preserve ABI
    if (struct_v >= 2) {
      decode(parent, p);
    }
    if (struct_v >= 3) {
      decode(protection_status, p);
    }
    if (struct_v >= 4) {
      decode(flags, p);
    }
    if (struct_v >= 5) {
      decode(snapshot_namespace, p);
    }
    if (struct_v >= 6) {
      decode(timestamp, p);
    }
    if (struct_v >= 7) {
      decode(child_count, p);
    }
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("id", id);
    f->dump_string("name", name);
    f->dump_unsigned("image_size", image_size);
    if (has_parent()) {
      f->open_object_section("parent");
      parent.dump(f);
      f->close_section();
    }
    switch (protection_status) {
    case RBD_PROTECTION_STATUS_UNPROTECTED:
      f->dump_string("protection_status", "unprotected");
      break;
    case RBD_PROTECTION_STATUS_UNPROTECTING:
      f->dump_string("protection_status", "unprotecting");
      break;
    case RBD_PROTECTION_STATUS_PROTECTED:
      f->dump_string("protection_status", "protected");
      break;
    default:
      ceph_abort();
    }
    f->dump_unsigned("child_count", child_count);
  }
  static void generate_test_instances(list<cls_rbd_snap*>& o) {
    o.push_back(new cls_rbd_snap);
    cls_rbd_snap *t = new cls_rbd_snap;
    t->id = 1;
    t->name = "snap";
    t->image_size = 123456;
    t->flags = 31;
    t->child_count = 543;
    o.push_back(t);
    t = new cls_rbd_snap;
    t->id = 2;
    t->name = "snap2";
    t->image_size = 12345678;
    t->parent.pool = 1;
    t->parent.id = "parent";
    t->parent.snapid = 456;
    t->parent.overlap = 12345;
    t->protection_status = RBD_PROTECTION_STATUS_PROTECTED;
    t->flags = 14;
    t->timestamp = utime_t();
    o.push_back(t);
  }
};
WRITE_CLASS_ENCODER(cls_rbd_snap)

#endif // __CEPH_CLS_RBD_H
