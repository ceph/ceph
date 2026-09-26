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

#include <optional>
#include <string>
#include <string_view>

#include "rgw_obj_types.h"

#include "fs_strategy.h"

namespace rgw { namespace sal { namespace nsfs {

/* Structural directory names.
 *
 * Used directly by the code which opens and creates them, which is
 * placement rather than naming and is deliberately not behind the
 * interface below -- where a bucket's tree is rooted, and what the
 * version and shadow subtrees are called, is entangled with the binding
 * site.  Declared here anyway so the reserved-name set and those call
 * sites cannot drift apart. */
inline constexpr std::string_view VERSIONS_DIR = ".versions";
inline constexpr std::string_view SHADOW_DIR = ".shadow";
inline constexpr std::string_view VERSIONS_LOCK = ".lock";
inline constexpr std::string_view FOLDER_OBJECT = ".folder";

/* What an object is called on disk, and what a name on disk means.
 *
 * Naming only.  *Where* a bucket's tree is rooted is deliberately not
 * here:  placement is entangled with where a POSIX identity binds, and
 * settling it inside a naming abstraction would answer that question by
 * accident.  See ACCOUNT_METADATA.md 0.2.
 *
 * Selected by the bucket's recorded format.  A tree named one way cannot
 * be read the other -- this decides whether an object called `_foo` is
 * the file `_foo` or the file `__foo` -- so it is a property of the data,
 * not of the machine. */
class PathStrategy {
public:
  virtual ~PathStrategy() = default;

  /* the file name for a key, within its bucket directory.  use_version
   * asks for the name carrying the version instance. */
  virtual std::string object_name(const rgw_obj_key& key,
				  bool use_version) const = 0;

  /* the inverse:  the key a file name represents */
  virtual rgw_obj_key key_from_name(const std::string& fname) const = 0;

  /* the directory a bucket lives in, given its name and namespace */
  virtual std::string bucket_dir_name(
      const std::string& name,
      const std::optional<std::string>& ns) const = 0;

  /* the sentinel naming a directory which is itself an object -- a key
   * ending in '/' has to be stored as something */
  virtual std::string folder_object_name() const = 0;

  /* Does this directory entry mean the containing directory is itself an
   * object?  The inverse of object_name()'s trailing-'/' rule.
   *
   * Asked rather than compared against folder_object_name(), because
   * recognition and construction are not the same question for every
   * format.  NooBaa writes the same `.folder` sentinel we do, but for an
   * *empty* directory object with versioning disabled it unlinks the
   * sentinel and marks the directory with user.noobaa.dir_content
   * instead (namespace_fs.js, _create_empty_dir_content).  Such a
   * directory object has no entry to recognise at all, so that format
   * will need a question about the DIRECTORY;  this is the entry-shaped
   * half, and the other half is S5's. */
  virtual bool names_directory_object(std::string_view entry) const = 0;

  /* names this layout creates which are not objects.  Contributed to the
   * driver's aggregate;  the listing paths match against that, so no
   * strategy is consulted per directory entry. */
  virtual const ReservedNames& reserved_names() const = 0;

  virtual const char* name() const = 0;
};

/* What nsfs writes today.
 *
 * An object is stored under its own name.  `rgw_obj_key::get_index_key_name()`
 * and `get_oid()` double a leading underscore -- in rados the index shares
 * a keyspace with entries spelled `_<ns>_<name>`, so a key beginning `_`
 * would collide -- and object_name() undoes that.  A directory has no
 * such keyspace, the doubling bought nothing here, and it stored the
 * user's object under a name the user did not choose and NooBaa does not
 * write.  Dropped in every profile, not only the noobaa one:  it is a
 * rados artifact rather than a format choice.
 *
 * Both halves had to move together, which is why they live on one
 * object;  key_from_name() no longer calls parse_raw_oid(), whose
 * namespace branch would mis-split a bare `_foo_bar`. */
class RGWPathStrategy : public PathStrategy {
public:
  std::string object_name(const rgw_obj_key& key,
			  bool use_version) const override;
  rgw_obj_key key_from_name(const std::string& fname) const override;
  std::string bucket_dir_name(
      const std::string& name,
      const std::optional<std::string>& ns) const override;
  std::string folder_object_name() const override;
  bool names_directory_object(std::string_view entry) const override;
  const ReservedNames& reserved_names() const override;

  const char* name() const override { return "rgw"; }
};

}}} // namespace rgw::sal::nsfs
