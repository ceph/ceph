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

#include "common/Formatter.h"

template<template<typename> class Allocator>
void inode_t<Allocator>::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_stream("btime") << btime;
  f->dump_unsigned("mode", mode);
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);

  f->open_object_section("dir_layout");
  ::dump(dir_layout, f);
  f->close_section();

  f->dump_object("layout", layout);

  f->open_array_section("old_pools");
  for (const auto &p : old_pools) {
    f->dump_int("pool", p);
  }
  f->close_section();

  f->dump_unsigned("size", size);
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_unsigned("truncate_from", truncate_from);
  f->dump_unsigned("truncate_pending", truncate_pending);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_unsigned("time_warp_seq", time_warp_seq);
  f->dump_unsigned("change_attr", change_attr);
  f->dump_int("export_pin", export_pin);
  f->dump_float("export_ephemeral_random_pin", export_ephemeral_random_pin);
  f->dump_bool("export_ephemeral_distributed_pin", get_ephemeral_distributed_pin());
  f->dump_bool("quiesce_block", get_quiesce_block());

  f->open_array_section("client_ranges");
  for (const auto &p : client_ranges) {
    f->open_object_section("client");
    f->dump_unsigned("client", p.first.v);
    p.second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("dirstat");
  dirstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();

  f->dump_unsigned("version", version);
  f->dump_unsigned("file_data_version", file_data_version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("backtrace_version", backtrace_version);
  f->dump_unsigned("inline_data_version", inline_data.version);
  f->dump_unsigned("inline_data_length", inline_data.length());

  f->dump_string("stray_prior_path", stray_prior_path);
  f->dump_unsigned("max_size_ever", max_size_ever);

  f->open_object_section("quota");
  quota.dump(f);
  f->close_section();

  f->dump_object("optmetadata", optmetadata);

  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_unsigned("last_scrub_version", last_scrub_version);
  f->dump_unsigned("remote_ino", remote_ino);
  f->open_array_section("referent_inodes");
  for (const auto &ri : referent_inodes) {
    f->dump_unsigned("referent_inode", ri);
  }
  f->close_section();
}

inline void vinodeno_t::dump(ceph::Formatter *f) const {
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("snapid", snapid);
}

template<template<typename> class Allocator>
void charmap_md_t<Allocator>::dump(ceph::Formatter* f) const {
  f->dump_bool("casesensitive", casesensitive);
  f->dump_string("normalization", normalization);
  f->dump_string("encoding", encoding);
}

template<template<typename> class Allocator>
void unknown_md_t<Allocator>::dump(ceph::Formatter* f) const {
  f->dump_bool("length", payload.size());
}

template<typename M, template<typename> class Allocator>
void optmetadata_singleton<M, Allocator>::dump(ceph::Formatter* f) const {
  f->dump_int("kind", u64kind);
  f->open_object_section("metadata");
  std::visit([f](auto& o) { o.dump(f); }, optmetadata);
  f->close_section();
}

template<typename Singleton, template<typename> class Allocator>
void optmetadata_multiton<Singleton, Allocator>::dump(ceph::Formatter* f) const {
  f->dump_bool("length", opts.size());
  f->open_array_section("opts");
  for (auto& opt : opts) {
    f->dump_object("opt", opt);
  }
  f->close_section();
}
