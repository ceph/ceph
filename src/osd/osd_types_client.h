// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TYPES_CLIENT_H
#define CEPH_OSD_TYPES_CLIENT_H

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "osd/osd_types_core.h"
#include "librados/ListObjectImpl.h"




/**
 * pg list objects response format
 *
 */

template<typename T>
struct pg_nls_response_template {
  collection_list_handle_t handle;
  std::vector<T> entries;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(handle, bl);
    __u32 n = (__u32)entries.size();
    encode(n, bl);
    for (auto i = entries.begin(); i != entries.end(); ++i) {
      encode(i->nspace, bl);
      encode(i->oid, bl);
      encode(i->locator, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(handle, bl);
    __u32 n;
    decode(n, bl);
    entries.clear();
    while (n--) {
      T i;
      decode(i.nspace, bl);
      decode(i.oid, bl);
      decode(i.locator, bl);
      entries.push_back(i);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (auto p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_string("namespace", p->nspace);
      f->dump_string("object", p->oid);
      f->dump_string("key", p->locator);
      f->close_section();
    }
    f->close_section();
  }
  static std::list<pg_nls_response_template> generate_test_instances() {
    std::list<pg_nls_response_template<T>> o;
    o.emplace_back();
    o.emplace_back();
    o.back().handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back().entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back().entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back().entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.emplace_back();
    o.back().handle = hobject_t(object_t("hi"), "key", 3, 4, -1, "");
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
    o.emplace_back();
    o.back().handle = hobject_t(object_t("hi"), "key", 5, 6, -1, "");
    o.back().entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back().entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back().entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back().entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
    return o;
  }
};

using pg_nls_response_t = pg_nls_response_template<librados::ListObjectImpl>;

WRITE_CLASS_ENCODER(pg_nls_response_t)

// For backwards compatibility with older OSD requests
struct pg_ls_response_t {
  collection_list_handle_t handle; 
  std::list<std::pair<object_t, std::string> > entries;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 v = 1;
    encode(v, bl);
    encode(handle, bl);
    encode(entries, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 v;
    decode(v, bl);
    ceph_assert(v == 1);
    decode(handle, bl);
    decode(entries, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (std::list<std::pair<object_t, std::string> >::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_stream("object") << p->first;
      f->dump_string("key", p->second);
      f->close_section();
    }
    f->close_section();
  }
  static std::list<pg_ls_response_t> generate_test_instances() {
    std::list<pg_ls_response_t> o;
    o.emplace_back();
    o.emplace_back();
    o.back().handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back().entries.push_back(std::make_pair(object_t("one"), std::string()));
    o.back().entries.push_back(std::make_pair(object_t("two"), std::string("twokey")));
    return o;
  }
};

WRITE_CLASS_ENCODER(pg_ls_response_t)

/**
 * object_copy_cursor_t
 */
struct object_copy_cursor_t {
  uint64_t data_offset;
  std::string omap_offset;
  bool attr_complete;
  bool data_complete;
  bool omap_complete;

  object_copy_cursor_t()
    : data_offset(0),
      attr_complete(false),
      data_complete(false),
      omap_complete(false)
  {}

  bool is_initial() const {
    return !attr_complete && data_offset == 0 && omap_offset.empty();
  }
  bool is_complete() const {
    return attr_complete && data_complete && omap_complete;
  }

  static std::list<object_copy_cursor_t> generate_test_instances();
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(object_copy_cursor_t)

/**
 * object_copy_data_t
 *
 * Return data from a copy request. The semantics are a little strange
 * as a result of the encoding's heritage.
 *
 * In particular, the sender unconditionally fills in the cursor (from what
 * it receives and sends), the size, and the mtime, but is responsible for
 * figuring out whether it should put any data in the attrs, data, or
 * omap members (corresponding to xattrs, object data, and the omap entries)
 * based on external data (the client includes a max amount to return with
 * the copy request). The client then looks into the attrs, data, and/or omap
 * based on the contents of the cursor.
 */
struct object_copy_data_t {
  enum {
    FLAG_DATA_DIGEST = 1<<0,
    FLAG_OMAP_DIGEST = 1<<1,
  };
  object_copy_cursor_t cursor;
  uint64_t size;
  utime_t mtime;
  uint32_t data_digest, omap_digest;
  uint32_t flags;
  std::map<std::string, ceph::buffer::list, std::less<>> attrs;
  ceph::buffer::list data;
  ceph::buffer::list omap_header;
  ceph::buffer::list omap_data;

  /// which snaps we are defined for (if a snap and not the head)
  std::vector<snapid_t> snaps;
  /// latest snap seq for the object (if head)
  snapid_t snap_seq;

  /// recent reqids on this object
  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > reqids;

  /// map reqids by index to error return code (if any)
  mempool::osd_pglog::map<uint32_t, int> reqid_return_codes;

  uint64_t truncate_seq;
  uint64_t truncate_size;

public:
  object_copy_data_t() :
    size((uint64_t)-1), data_digest(-1),
    omap_digest(-1), flags(0),
    truncate_seq(0),
    truncate_size(0) {}

  static std::list<object_copy_data_t> generate_test_instances();
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER_FEATURES(object_copy_data_t)


// -----------------------------------------

class ObjectExtent {
  /**
   * ObjectExtents are used for specifying IO behavior against RADOS
   * objects when one is using the ObjectCacher.
   *
   * To use this in a real system, *every member* must be filled
   * out correctly. In particular, make sure to initialize the
   * oloc correctly, as its default values are deliberate poison
   * and will cause internal ObjectCacher asserts.
   *
   * Similarly, your buffer_extents vector *must* specify a total
   * size equal to your length. If the buffer_extents inadvertently
   * contain less space than the length member specifies, you
   * will get unintelligible asserts deep in the ObjectCacher.
   *
   * If you are trying to do testing and don't care about actual
   * RADOS function, the simplest thing to do is to initialize
   * the ObjectExtent (truncate_size can be 0), create a single entry
   * in buffer_extents matching the length, and set oloc.pool to 0.
   */
 public:
  object_t    oid;       // object id
  uint64_t    objectno;
  uint64_t    offset;    // in object
  uint64_t    length;    // in object
  uint64_t    truncate_size;	// in object

  object_locator_t oloc;   // object locator (pool etc)

  std::vector<std::pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0), truncate_size(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l, uint64_t ts) :
    oid(o), objectno(ono), offset(off), length(l), truncate_size(ts) { }
};

inline std::ostream& operator<<(std::ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " (" << ex.objectno << ") in " << ex.oloc
             << " " << ex.offset << "~" << ex.length
	     << " -> " << ex.buffer_extents
             << ")";
}



struct watch_item_t {
  entity_name_t name;
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_item_t() : cookie(0), timeout_seconds(0) { }
  watch_item_t(entity_name_t name, uint64_t cookie, uint32_t timeout,
     const entity_addr_t& addr)
    : name(name), cookie(cookie), timeout_seconds(timeout),
    addr(addr) { }

  void encode(ceph::buffer::list &bl, uint64_t features) const {
    ENCODE_START(2, 1, bl);
    encode(name, bl);
    encode(cookie, bl);
    encode(timeout_seconds, bl);
    encode(addr, bl, features);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(2, bl);
    decode(name, bl);
    decode(cookie, bl);
    decode(timeout_seconds, bl);
    if (struct_v >= 2) {
      decode(addr, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("watcher") << name;
    f->dump_int("cookie", cookie);
    f->dump_int("timeout", timeout_seconds);
    f->open_object_section("addr");
    addr.dump(f);
    f->close_section();
  }
  static std::list<watch_item_t> generate_test_instances() {
    std::list<watch_item_t> o;
    entity_addr_t ea;
    ea.set_type(entity_addr_t::TYPE_LEGACY);
    ea.set_nonce(1000);
    ea.set_family(AF_INET);
    ea.set_in4_quad(0, 127);
    ea.set_in4_quad(1, 0);
    ea.set_in4_quad(2, 0);
    ea.set_in4_quad(3, 1);
    ea.set_port(1024);
    o.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 1), 10, 30, ea));
    ea.set_nonce(1001);
    ea.set_in4_quad(3, 2);
    ea.set_port(1025);
    o.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 2), 20, 60, ea));
    return o;
  }
};
WRITE_CLASS_ENCODER_FEATURES(watch_item_t)

struct obj_watch_item_t {
  hobject_t obj;
  watch_item_t wi;
};

/**
 * obj list watch response format
 *
 */
struct obj_list_watch_response_t {
  std::list<watch_item_t> entries;

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl, features);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("entries");
    for (std::list<watch_item_t>::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("watch");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static std::list<obj_list_watch_response_t> generate_test_instances() {
    std::list<obj_list_watch_response_t> o;
    entity_addr_t ea;
    o.emplace_back();
    o.emplace_back();
    std::list<watch_item_t> test_watchers = watch_item_t::generate_test_instances();
    for (auto &e : test_watchers) {
      o.back().entries.push_back(e);
    }
    return o;
  }
};
WRITE_CLASS_ENCODER_FEATURES(obj_list_watch_response_t)

struct clone_info {
  snapid_t cloneid;
  std::vector<snapid_t> snaps;  // ascending
  std::vector< std::pair<uint64_t,uint64_t> > overlap;
  uint64_t size;

  clone_info() : cloneid(CEPH_NOSNAP), size(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(cloneid, bl);
    encode(snaps, bl);
    encode(overlap, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(cloneid, bl);
    decode(snaps, bl);
    decode(overlap, bl);
    decode(size, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    if (cloneid == CEPH_NOSNAP)
      f->dump_string("cloneid", "HEAD");
    else
      f->dump_unsigned("cloneid", cloneid.val);
    f->open_array_section("snapshots");
    for (std::vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
      f->open_object_section("snap");
      f->dump_unsigned("id", p->val);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("overlaps");
    for (std::vector< std::pair<uint64_t,uint64_t> >::const_iterator q = overlap.begin();
          q != overlap.end(); ++q) {
      f->open_object_section("overlap");
      f->dump_unsigned("offset", q->first);
      f->dump_unsigned("length", q->second);
      f->close_section();
    }
    f->close_section();
    f->dump_unsigned("size", size);
  }
  static std::list<clone_info> generate_test_instances() {
    std::list<clone_info> o;
    o.emplace_back();
    o.emplace_back();
    o.back().cloneid = 1;
    o.back().snaps.push_back(1);
    o.back().overlap.push_back(std::pair<uint64_t,uint64_t>(0,4096));
    o.back().overlap.push_back(std::pair<uint64_t,uint64_t>(8192,4096));
    o.back().size = 16384;
    o.emplace_back();
    o.back().cloneid = CEPH_NOSNAP;
    o.back().size = 32768;
    return o;
  }
};
WRITE_CLASS_ENCODER(clone_info)

/**
 * obj list snaps response format
 *
 */
struct obj_list_snap_response_t {
  std::vector<clone_info> clones;   // ascending
  snapid_t seq;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(clones, bl);
    encode(seq, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(clones, bl);
    if (struct_v >= 2)
      decode(seq, bl);
    else
      seq = CEPH_NOSNAP;
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("clones");
    for (std::vector<clone_info>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
      f->open_object_section("clone");
      p->dump(f);
      f->close_section();
    }
    f->dump_unsigned("seq", seq);
    f->close_section();
  }
  static std::list<obj_list_snap_response_t> generate_test_instances() {
    std::list<obj_list_snap_response_t> o;
    o.emplace_back();
    o.emplace_back();
    clone_info cl;
    cl.cloneid = 1;
    cl.snaps.push_back(1);
    cl.overlap.push_back(std::pair<uint64_t,uint64_t>(0,4096));
    cl.overlap.push_back(std::pair<uint64_t,uint64_t>(8192,4096));
    cl.size = 16384;
    o.back().clones.push_back(cl);
    cl.cloneid = CEPH_NOSNAP;
    cl.snaps.clear();
    cl.overlap.clear();
    cl.size = 32768;
    o.back().clones.push_back(cl);
    o.back().seq = 123;
    return o;
  }
};

WRITE_CLASS_ENCODER(obj_list_snap_response_t)

#endif // CEPH_OSD_TYPES_CLIENT_H
