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

  /* Is this directory itself an object?
   *
   * The other half the comment above anticipated.  A format which
   * marks the directory rather than putting a sentinel inside it has
   * no entry to recognise, so the question has to be asked one level
   * up, with the parent's fd and the directory's name.
   *
   * Ours answers false without a syscall:  we never mark a directory,
   * the sentinel is the marker, and the walk sees it for free while
   * enumerating.  A bucket we wrote holds no foreign directory
   * objects, so paying per directory on the listing path would be
   * paying for a case that cannot occur there.
   *
   * NooBaa reads user.noobaa.dir_content off the directory, whose
   * value is the content length as a string.  "0" means the directory
   * alone is the object and there is no sentinel file;  non-zero means
   * the bytes are in the sentinel and the directory's own attributes
   * are the object's (`namespace_fs.js:1008`).  A directory without
   * the attribute is not an object at all, which is why its absence
   * cannot be inferred from a missing sentinel. */
  struct DirectoryObject {
    uint64_t size{0};
    /* the bytes are in folder_object_name();  false means the object
     * is empty and the directory is the whole of it */
    bool content_in_sentinel{false};
  };

  /* `dir_fd` is the directory itself, not its parent.  The listing
   * walk already opens every subdirectory before recursing into it, so
   * asking there costs one fgetxattr on a descriptor it holds -- the
   * openat and close a parent-relative form would need are what made
   * the probe look expensive. */
  virtual bool directory_object(const DoutPrefixProvider* dpp,
				int dir_fd,
				DirectoryObject& out) const = 0;

  /* Stop this directory being an object in this format.
   *
   * The counterpart of directory_object(), for the upgrade:  a
   * directory carried across to another format must stop answering in
   * the one it came from, and which attribute says so is the format's
   * business and not the converter's.  Ours has nothing to clear --
   * the sentinel file is the marker and removing it is a delete, not a
   * conversion -- so ours does nothing. */
  virtual int clear_directory_object(const DoutPrefixProvider* dpp,
				     int dir_fd) const = 0;

  /* names this layout creates which are not objects.  Contributed to the
   * driver's aggregate;  the listing paths match against that, so no
   * strategy is consulted per directory entry. */
  virtual const ReservedNames& reserved_names() const = 0;

  virtual const char* name() const = 0;
};

/* What nsfs writes today.
 *
 * Named for the sentinel, which is the thin part of the contrast:  both
 * formats store an object under its own name and both write `.folder`,
 * and the only divergence identified is that NooBaa may record an empty
 * directory object as a directory attribute instead of an entry.  The
 * question which handles that is S5's (see names_directory_object), so
 * this name may want revisiting once the contrast is real.
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
class SentinelPathStrategy : public PathStrategy {
public:
  std::string object_name(const rgw_obj_key& key,
			  bool use_version) const override;
  rgw_obj_key key_from_name(const std::string& fname) const override;
  std::string bucket_dir_name(
      const std::string& name,
      const std::optional<std::string>& ns) const override;
  std::string folder_object_name() const override;
  bool names_directory_object(std::string_view entry) const override;
  bool directory_object(const DoutPrefixProvider* dpp, int dir_fd,
			DirectoryObject& out) const override;
  int clear_directory_object(const DoutPrefixProvider* dpp,
			     int dir_fd) const override;
  const ReservedNames& reserved_names() const override;

  const char* name() const override { return "rgw"; }
};

/* NooBaa's layout, which is what the base profile is.
 *
 * Most of it is already ours.  Object keys are stored verbatim on both
 * sides -- the rados underscore doubling went in `7f0c721ab56`, in
 * every profile -- and `.folder` and `.versions` are theirs, adopted.
 * So object_name(), key_from_name() and folder_object_name() are the
 * same answers, and this exists for three differences.
 *
 * reserved_names() has no `.shadow`, because they have no shadow
 * subtree, and gains `.noobaa-nsfs` so the bucket temp directory is
 * never enumerated as a key.
 *
 * bucket_dir_name() does not render our staging spelling.  Their
 * staging is under the temp directory, which MPUStrategy::
 * staging_root() finds;  nothing beside the bucket is named for an
 * upload.
 *
 * directory_object() reads user.noobaa.dir_content.  This is the one
 * that matters for interop:  a zero-length directory object of theirs
 * has no sentinel at all, so without it a trailing-slash key they
 * created is invisible to us, and one we create is invisible to them.
 * That is the folder case, which is how most tools make folders.
 *
 * Read from noobaa-core at 68ca22d33:  `config.NSFS_FOLDER_OBJECT_NAME`
 * is '.folder', and `namespace_fs.js:1008` is the read path. */
class NooBaaPathStrategy : public PathStrategy {
public:
  std::string object_name(const rgw_obj_key& key,
			  bool use_version) const override;
  rgw_obj_key key_from_name(const std::string& fname) const override;
  std::string bucket_dir_name(
      const std::string& name,
      const std::optional<std::string>& ns) const override;
  std::string folder_object_name() const override;
  bool names_directory_object(std::string_view entry) const override;
  bool directory_object(const DoutPrefixProvider* dpp, int dir_fd,
			DirectoryObject& out) const override;
  int clear_directory_object(const DoutPrefixProvider* dpp,
			     int dir_fd) const override;
  const ReservedNames& reserved_names() const override;

  const char* name() const override { return "noobaa"; }
};

}}} // namespace rgw::sal::nsfs
