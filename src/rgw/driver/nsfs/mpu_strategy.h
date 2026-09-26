// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include "fs_strategy.h"

class DoutPrefixProvider;

namespace rgw { namespace sal { namespace nsfs {

/* Where a multipart upload's parts live while it is in flight, what they
 * are called, and what completing it does.
 *
 * Separate from FSStrategy, which is a mechanism abstraction and owns no
 * naming.  This owns the layout:  the questions below were answered by
 * string concatenation at each use site, which is why the staging
 * directory name was built two different ways for the same result.
 *
 * Why these belong to one object rather than two.  Where the staging tree
 * goes and how parts are stored travel together:  writing each part
 * straight into its final offset does not merely change file contents, it
 * changes which files exist.  Splitting them would give two objects
 * sharing one selection key.
 *
 * Selection is DERIVED from what the filesystem can do, not recorded.
 * Assembly is a copy_file_range per part;  where extents are shared that
 * is nearly free and per-part files cost nothing, and where they are not
 * -- GPFS answers EOPNOTSUPP at every granularity, measured on Storage
 * Scale 6.0.0.2 -- completing an upload rewrites every byte, and writing
 * parts into their final offsets so that complete is a rename is the
 * better layout.  Both answers are known and they differ, so the chooser
 * asks FSStrategy.
 *
 * One implementation today, deliberately:  it is the layout nsfs already
 * writes, moved behind an interface without changing a byte of it. */
class MPUStrategy {
public:
  virtual ~MPUStrategy() = default;

  /* the per-upload staging directory, under the bucket */
  virtual std::string staging_dir_name(const std::string& upload_id) const = 0;

  /* a part's file name, and the inverse.  part_number() returns nullopt
   * for a name which is not a part, so a caller does not have to know the
   * spelling to ask. */
  /* Is this directory entry a staging directory?
   *
   * Asked rather than compared against staging_dir_name(), for the same
   * reason is_part_name() is separate from part_name():  recognition and
   * construction are not the same question for every format, and only
   * this side has to work on a tree somebody else wrote.
   *
   * This replaced a stored attribute.  nsfs used to write an
   * `object_type` xattr on every object -- inherited from the posix
   * driver, which still has it as POSIX-Object-Type -- and read it back
   * when enumerating a directory, to tell a staging directory from an
   * ordinary one.  That is a cached answer to a question the name
   * already settles, it cost an openat and an attribute read per
   * directory entry, and it could not work on a NooBaa-format tree,
   * whose staging directories carry no attribute of ours. */
  virtual bool names_staging_dir(std::string_view name) const = 0;

  virtual std::string part_name(uint32_t part_num) const = 0;
  virtual bool is_part_name(std::string_view name) const = 0;
  virtual std::optional<uint32_t> part_number(std::string_view name) const = 0;

  /* fixed names inside the staging directory */
  virtual std::string head_name() const = 0;
  virtual std::string meta_name() const = 0;
  virtual std::string assembled_name() const = 0;

  /* names this layout creates which are scaffolding rather than objects,
   * so that a layout which stages elsewhere does not have to teach the
   * listing paths about its own names.  The reference outlives the call. */
  virtual const ReservedNames& reserved_names() const = 0;

  /* make the completed object from its parts.  dir_fd is the staging
   * directory;  output_name is created within it.  fs supplies the copy,
   * which is the whole reason the answer depends on the filesystem. */
  virtual int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
                       int dir_fd, int num_parts,
                       const std::string& output_name) const = 0;

  virtual const char* name() const = 0;
};

/* The layout nsfs writes today:  <bucket>/.multipart_<upload_id>/ holding
 * part-NNNNN, .meta and .assembled, assembled by copying each part into a
 * single output file.
 *
 * The zero padding is load-bearing for nothing -- consumers construct the
 * name from a number or parse it with std::stoul, and ordering comes from
 * a flat_map keyed on the number.  It is kept because this format is ours
 * and changing a name here would cost the only cheap proof that moving
 * these decisions behind an interface changed nothing. */
class RGWMPUStrategy : public MPUStrategy {
public:
  std::string staging_dir_name(const std::string& upload_id) const override;

  bool names_staging_dir(std::string_view name) const override;
  std::string part_name(uint32_t part_num) const override;
  bool is_part_name(std::string_view name) const override;
  std::optional<uint32_t> part_number(std::string_view name) const override;

  std::string head_name() const override;
  std::string meta_name() const override;
  std::string assembled_name() const override;

  const ReservedNames& reserved_names() const override;

  int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
               int dir_fd, int num_parts,
               const std::string& output_name) const override;

  const char* name() const override { return "rgw"; }
};

}}} // namespace rgw::sal::nsfs
