// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "types.h"

#include "common/ceph_json.h"

template<template<typename> class Allocator>
void inode_t<Allocator>::client_ranges_cb(typename inode_t<Allocator>::client_range_map& c, JSONObj *obj){

  int64_t client;
  JSONDecoder::decode_json("client", client, obj, true);
  client_writeable_range_t client_range_tmp;
  JSONDecoder::decode_json("byte range", client_range_tmp.range, obj, true);
  JSONDecoder::decode_json("follows", client_range_tmp.follows.val, obj, true);
  c[client] = client_range_tmp;
}

template<template<typename> class Allocator>
void inode_t<Allocator>::old_pools_cb(compact_set<int64_t, std::less<int64_t>, Allocator<int64_t> >& c, JSONObj *obj){

  int64_t tmp;
  decode_json_obj(tmp, obj);
  c.insert(tmp);
}

template<template<typename> class Allocator>
void inode_t<Allocator>::decode_json(JSONObj *obj)
{

  JSONDecoder::decode_json("ino", ino.val, obj, true);
  JSONDecoder::decode_json("rdev", rdev, obj, true);
  //JSONDecoder::decode_json("ctime", ctime, obj, true);
  //JSONDecoder::decode_json("btime", btime, obj, true);
  JSONDecoder::decode_json("mode", mode, obj, true);
  JSONDecoder::decode_json("uid", uid, obj, true);
  JSONDecoder::decode_json("gid", gid, obj, true);
  JSONDecoder::decode_json("nlink", nlink, obj, true);
  JSONDecoder::decode_json("dir_layout", dir_layout, obj, true);
  JSONDecoder::decode_json("layout", layout, obj, true);
  JSONDecoder::decode_json("old_pools", old_pools, inode_t<Allocator>::old_pools_cb, obj, true);
  JSONDecoder::decode_json("size", size, obj, true);
  JSONDecoder::decode_json("truncate_seq", truncate_seq, obj, true);
  JSONDecoder::decode_json("truncate_size", truncate_size, obj, true);
  JSONDecoder::decode_json("truncate_from", truncate_from, obj, true);
  JSONDecoder::decode_json("truncate_pending", truncate_pending, obj, true);
  //JSONDecoder::decode_json("mtime", mtime, obj, true);
  //JSONDecoder::decode_json("atime", atime, obj, true);
  JSONDecoder::decode_json("time_warp_seq", time_warp_seq, obj, true);
  JSONDecoder::decode_json("change_attr", change_attr, obj, true);
  JSONDecoder::decode_json("export_pin", export_pin, obj, true);
  JSONDecoder::decode_json("client_ranges", client_ranges, inode_t<Allocator>::client_ranges_cb, obj, true);
  JSONDecoder::decode_json("dirstat", dirstat, obj, true);
  JSONDecoder::decode_json("rstat", rstat, obj, true);
  JSONDecoder::decode_json("accounted_rstat", accounted_rstat, obj, true);
  JSONDecoder::decode_json("version", version, obj, true);
  JSONDecoder::decode_json("file_data_version", file_data_version, obj, true);
  JSONDecoder::decode_json("xattr_version", xattr_version, obj, true);
  JSONDecoder::decode_json("backtrace_version", backtrace_version, obj, true);
  JSONDecoder::decode_json("stray_prior_path", stray_prior_path, obj, true);
  JSONDecoder::decode_json("max_size_ever", max_size_ever, obj, true);
  JSONDecoder::decode_json("quota", quota, obj, true);
  JSONDecoder::decode_json("last_scrub_stamp", last_scrub_stamp, obj, true);
  JSONDecoder::decode_json("last_scrub_version", last_scrub_version, obj, true);
  JSONDecoder::decode_json("remote_ino", remote_ino.val, obj, true);
  JSONDecoder::decode_json("referent_inodes", referent_inodes, obj, true);
}
