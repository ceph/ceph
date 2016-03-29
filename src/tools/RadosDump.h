// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RADOS_DUMP_H_
#define RADOS_DUMP_H_

#include <stdint.h>

#include "include/buffer.h"
#include "include/encoding.h"

#include "osd/osd_types.h"
#include "osd/OSDMap.h"

typedef uint8_t sectiontype_t;
typedef uint32_t mymagic_t;
typedef int64_t mysize_t;

enum {
    TYPE_NONE = 0,
    TYPE_PG_BEGIN,
    TYPE_PG_END,
    TYPE_OBJECT_BEGIN,
    TYPE_OBJECT_END,
    TYPE_DATA,
    TYPE_ATTRS,
    TYPE_OMAP_HDR,
    TYPE_OMAP,
    TYPE_PG_METADATA,
    TYPE_POOL_BEGIN,
    TYPE_POOL_END,
    END_OF_TYPES,	//Keep at the end
};

const uint16_t shortmagic = 0xffce;	//goes into stream as "ceff"
//endmagic goes into stream as "ceff ffec"
const mymagic_t endmagic = (0xecff << 16) | shortmagic;

//The first FIXED_LENGTH bytes are a fixed
//portion of the export output.  This includes the overall
//version number, and size of header and footer.
//THIS STRUCTURE CAN ONLY BE APPENDED TO.  If it needs to expand,
//the version can be bumped and then anything
//can be added to the export format.
struct super_header {
  static const uint32_t super_magic = (shortmagic << 16) | shortmagic;
  // ver = 1, Initial version
  // ver = 2, Add OSDSuperblock to pg_begin
  static const uint32_t super_ver = 2;
  static const uint32_t FIXED_LENGTH = 16;
  uint32_t magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t footer_size;

  super_header() : magic(0), version(0), header_size(0), footer_size(0) { }

  void encode(bufferlist& bl) const {
    ::encode(magic, bl);
    ::encode(version, bl);
    ::encode(header_size, bl);
    ::encode(footer_size, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(magic, bl);
    ::decode(version, bl);
    ::decode(header_size, bl);
    ::decode(footer_size, bl);
  }
};

struct header {
  sectiontype_t type;
  mysize_t size;
  header(sectiontype_t type, mysize_t size) :
    type(type), size(size) { }
  header(): type(0), size(0) { }

  void encode(bufferlist& bl) const {
    uint32_t debug_type = (type << 24) | (type << 16) | shortmagic;
    ENCODE_START(1, 1, bl);
    ::encode(debug_type, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint32_t debug_type;
    DECODE_START(1, bl);
    ::decode(debug_type, bl);
    type = debug_type >> 24;
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
};

struct footer {
  mymagic_t magic;
  footer() : magic(endmagic) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(magic, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(magic, bl);
    DECODE_FINISH(bl);
  }
};

struct pg_begin {
  spg_t pgid;
  OSDSuperblock superblock;

  pg_begin(spg_t pg, const OSDSuperblock& sb):
    pgid(pg), superblock(sb) { }
  pg_begin() { }

  void encode(bufferlist& bl) const {
    // If superblock doesn't include CEPH_FS_FEATURE_INCOMPAT_SHARDS then
    // shard will be NO_SHARD for a replicated pool.  This means
    // that we allow the decode by struct_v 2.
    ENCODE_START(3, 2, bl);
    ::encode(pgid.pgid, bl);
    ::encode(superblock, bl);
    ::encode(pgid.shard, bl);
    ENCODE_FINISH(bl);
  }
  // NOTE: New super_ver prevents decode from ver 1
  void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    ::decode(pgid.pgid, bl);
    if (struct_v > 1) {
      ::decode(superblock, bl);
    }
    if (struct_v > 2) {
      ::decode(pgid.shard, bl);
    } else {
      pgid.shard = shard_id_t::NO_SHARD;
    }
    DECODE_FINISH(bl);
  }
};

struct object_begin {
  ghobject_t hoid;

  // Duplicate what is in the OI_ATTR so we have it at the start
  // of object processing.
  object_info_t oi;

  explicit object_begin(const ghobject_t &hoid): hoid(hoid) { }
  object_begin() { }

  // If superblock doesn't include CEPH_FS_FEATURE_INCOMPAT_SHARDS then
  // generation will be NO_GEN, shard_id will be NO_SHARD for a replicated
  // pool.  This means we will allow the decode by struct_v 1.
  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    ::encode(hoid.hobj, bl);
    ::encode(hoid.generation, bl);
    ::encode(hoid.shard_id, bl);
    ::encode(oi, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    ::decode(hoid.hobj, bl);
    if (struct_v > 1) {
      ::decode(hoid.generation, bl);
      ::decode(hoid.shard_id, bl);
    } else {
      hoid.generation = ghobject_t::NO_GEN;
      hoid.shard_id = shard_id_t::NO_SHARD;
    }
    if (struct_v > 2) {
      ::decode(oi, bl);
    }
    DECODE_FINISH(bl);
  }
};

struct data_section {
  uint64_t offset;
  uint64_t len;
  bufferlist databl;
  data_section(uint64_t offset, uint64_t len, bufferlist bl):
     offset(offset), len(len), databl(bl) { }
  data_section(): offset(0), len(0) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(offset, bl);
    ::encode(len, bl);
    ::encode(databl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(offset, bl);
    ::decode(len, bl);
    ::decode(databl, bl);
    DECODE_FINISH(bl);
  }
};

struct attr_section {
  map<string,bufferlist> data;
  explicit attr_section(const map<string,bufferlist> &data) : data(data) { }
  explicit attr_section(map<string, bufferptr> &data_)
  {
    for (std::map<std::string, bufferptr>::iterator i = data_.begin();
         i != data_.end(); ++i) {
      bufferlist bl;
      bl.push_front(i->second);
      data[i->first] = bl;
    }
  }

  attr_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_hdr_section {
  bufferlist hdr;
  explicit omap_hdr_section(bufferlist hdr) : hdr(hdr) { }
  omap_hdr_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(hdr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(hdr, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_section {
  map<string, bufferlist> omap;
  explicit omap_section(const map<string, bufferlist> &omap) :
    omap(omap) { }
  omap_section() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(omap, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(omap, bl);
    DECODE_FINISH(bl);
  }
};

struct metadata_section {
  // struct_ver is the on-disk version of original pg
  __u8 struct_ver;  // for reference
  epoch_t map_epoch;
  pg_info_t info;
  pg_log_t log;
  map<epoch_t,pg_interval_t> past_intervals;
  OSDMap osdmap;
  bufferlist osdmap_bl;  // Used in lieu of encoding osdmap due to crc checking
  map<eversion_t, hobject_t> divergent_priors;

  metadata_section(__u8 struct_ver, epoch_t map_epoch, const pg_info_t &info,
		   const pg_log_t &log, map<epoch_t,pg_interval_t> &past_intervals,
		   map<eversion_t, hobject_t> &divergent_priors)
    : struct_ver(struct_ver),
      map_epoch(map_epoch),
      info(info),
      log(log),
      past_intervals(past_intervals),
      divergent_priors(divergent_priors) { }
  metadata_section()
    : struct_ver(0),
      map_epoch(0) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    ::encode(struct_ver, bl);
    ::encode(map_epoch, bl);
    ::encode(info, bl);
    ::encode(log, bl);
    ::encode(past_intervals, bl);
    // Equivalent to osdmap.encode(bl, features); but
    // preserving exact layout for CRC checking.
    bl.append(osdmap_bl);
    ::encode(divergent_priors, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(4, bl);
    ::decode(struct_ver, bl);
    ::decode(map_epoch, bl);
    ::decode(info, bl);
    ::decode(log, bl);
    if (struct_v > 1) {
      ::decode(past_intervals, bl);
    } else {
      cout << "NOTICE: Older export without past_intervals" << std::endl;
    }
    if (struct_v > 2) {
      osdmap.decode(bl);
    } else {
      cout << "WARNING: Older export without OSDMap information" << std::endl;
    }
    if (struct_v > 3) {
      ::decode(divergent_priors, bl);
    }
    DECODE_FINISH(bl);
  }
};

/**
 * Superclass for classes that will need to handle a serialized RADOS
 * dump.  Requires that the serialized dump be opened with a known FD.
 */
class RadosDump
{
  protected:
    int file_fd;
    super_header sh;
    bool dry_run;

  public:
    RadosDump(int file_fd_, bool dry_run_)
      : file_fd(file_fd_), dry_run(dry_run_)
    {}

    int read_super();
    int get_header(header *h);
    int get_footer(footer *f);
    int read_section(sectiontype_t *type, bufferlist *bl);
    int skip_object(bufferlist &bl);
    void write_super();

    // Define this in .h because it's templated
    template <typename T>
      int write_section(sectiontype_t type, const T& obj, int fd) {
        if (dry_run)
          return 0;
        bufferlist blhdr, bl, blftr;
        obj.encode(bl);
        header hdr(type, bl.length());
        hdr.encode(blhdr);
        footer ft;
        ft.encode(blftr);

        int ret = blhdr.write_fd(fd);
        if (ret) return ret;
        ret = bl.write_fd(fd);
        if (ret) return ret;
        ret = blftr.write_fd(fd);
        return ret;
      }

    int write_simple(sectiontype_t type, int fd)
    {
      if (dry_run)
        return 0;
      bufferlist hbl;

      header hdr(type, 0);
      hdr.encode(hbl);
      return hbl.write_fd(fd);
    }
};

#endif
