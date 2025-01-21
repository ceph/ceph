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

#include "include/encoding.h"

/**
 * Value class for the xattr we'll use to accumulate
 * the highest object seen for a given inode
 */
class ObjCeiling {
  public:
    uint64_t id;
    uint64_t size;

    ObjCeiling()
      : id(0), size(0)
    {}

    ObjCeiling(uint64_t id_, uint64_t size_)
      : id(id_), size(size_)
    {}

    bool operator >(ObjCeiling const &rhs) const
    {
      return id > rhs.id;
    }

    void encode(ceph::buffer::list &bl) const
    {
      ENCODE_START(1, 1, bl);
      encode(id, bl);
      encode(size, bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &p)
    {
      DECODE_START(1, p);
      decode(id, p);
      decode(size, p);
      DECODE_FINISH(p);
    }
};
WRITE_CLASS_ENCODER(ObjCeiling)

class AccumulateArgs
{
public:
  uint64_t obj_index;
  uint64_t obj_size;
  int64_t mtime;
  std::string obj_xattr_name;
  std::string mtime_xattr_name;
  std::string obj_size_xattr_name;

  AccumulateArgs(
      uint64_t obj_index_,
      uint64_t obj_size_,
      time_t mtime_,
      const std::string &obj_xattr_name_,
      const std::string &mtime_xattr_name_,
      const std::string &obj_size_xattr_name_)
   : obj_index(obj_index_),
     obj_size(obj_size_),
     mtime(mtime_),
     obj_xattr_name(obj_xattr_name_),
     mtime_xattr_name(mtime_xattr_name_),
     obj_size_xattr_name(obj_size_xattr_name_)
  {}

  AccumulateArgs()
    : obj_index(0), obj_size(0), mtime(0)
  {}

  void encode(ceph::buffer::list &bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(obj_xattr_name, bl);
    encode(mtime_xattr_name, bl);
    encode(obj_size_xattr_name, bl);
    encode(obj_index, bl);
    encode(obj_size, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl)
  {
    DECODE_START(1, bl);
    decode(obj_xattr_name, bl);
    decode(mtime_xattr_name, bl);
    decode(obj_size_xattr_name, bl);
    decode(obj_index, bl);
    decode(obj_size, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
};

class InodeTagFilterArgs
{
  public:
    std::string scrub_tag;

  void encode(ceph::buffer::list &bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(scrub_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl)
  {
    DECODE_START(1, bl);
    decode(scrub_tag, bl);
    DECODE_FINISH(bl);
  }
};

class AccumulateResult
{
public:
  // Index of the highest-indexed object seen
  uint64_t ceiling_obj_index;
  // Size of the highest-index object seen
  uint64_t ceiling_obj_size;
  // Largest object seen
  uint64_t max_obj_size;
  // Non-default object pool id seen
  int64_t obj_pool_id;
  // Highest mtime seen
  int64_t   max_mtime;

  AccumulateResult()
    : ceiling_obj_index(0), ceiling_obj_size(0), max_obj_size(0),
      obj_pool_id(-1), max_mtime(0)
  {}
};

