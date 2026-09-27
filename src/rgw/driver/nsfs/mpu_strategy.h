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
 * parts into their final offsets so that complete is a link is the
 * better layout.  Both answers are known and they differ, so the chooser
 * asks FSStrategy.
 *
 * Two implementations:  PerPartMPUStrategy, one file per part, and
 * StridedMPUStrategy, which derives from it and adds the shared data
 * file.  A NooBaa one is not written yet. */
class MPUStrategy {
public:
  virtual ~MPUStrategy() = default;

  /* The per-upload staging directory, under the bucket.
   *
   * Named for the upload's meta -- the object key and the upload id --
   * so that enumerating the bucket recovers both from the directory
   * entry.  A name carrying the upload id alone costs an open and a
   * metadata read per upload to answer "which object is this", which
   * is what ListMultipartUploads has to know before it can order or
   * page.
   *
   * The meta is encoded, because a key contains '/' and the staging
   * directory is one path component.  Object names are stored
   * verbatim (see PathStrategy) and this does not reopen that:  a
   * staging directory is scaffolding, and the key inside it is not a
   * path. */
  virtual std::string staging_dir_name(const std::string& meta) const = 0;

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

  /* Where a part's bytes go.
   *
   * A layout which gives every part its own file answers with that file
   * and offset zero.  A layout which shares one file between the parts
   * of a size answers with that file and the part's offset within it.
   *
   * Separate from part_name(), which names the part's *record*.  The two
   * coincide in the layout nsfs writes today and do not have to:  a
   * shared data file leaves part_name() holding metadata and nothing
   * else.
   *
   * stride is the upload's established uniform part size -- part 1's
   * stored length, once part 1 has completed and established it -- and
   * std::nullopt before that, or where the upload was disqualified.  A
   * layout which cannot place a part without a stride answers nullopt
   * in turn, and the caller stages the part in its own file.
   *
   * NOT the part's own length, which nobody knows before the body has
   * been read:  compression and AEAD sit between the op layer and the
   * writer, so the only true count is the one the writer accumulates.
   *
   * extent bounds what the part may write.  A shared file gives a part
   * one stride and no more, because exceeding it would overrun the next
   * part's region;  the writer refuses and diverts rather than
   * discovering the damage afterwards.  std::nullopt is unbounded,
   * which is what a part with its own file gets. */
  struct PartTarget {
    std::string name;                /* file within the staging directory */
    uint64_t offset{0};              /* where this part's bytes begin */
    std::optional<uint64_t> extent;  /* how much it may write there */
    bool shared{false};              /* other parts write into this file */
  };

  virtual std::optional<PartTarget> part_target(
      uint32_t part_num, std::optional<uint64_t> stride) const = 0;

  /* A part as assembly sees it:  which part, where its bytes are, and
   * how many.  Read from the part records by the caller, which owns the
   * attribute format;  this interface does not.
   *
   * `stored` is bytes on disk, not the accounted length the client
   * sent.  A layout which gives every part its own file may ignore
   * `shared` and `offset` entirely. */
  struct PartPlacement {
    uint32_t num{0};
    bool shared{false};
    uint64_t offset{0};
    uint64_t stored{0};
  };

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
   * which is the whole reason the answer depends on the filesystem.
   *
   * parts must be in ascending part order:  the output is their
   * concatenation in that order, and a layout which places parts by
   * offset can only check its placement against a sorted run. */
  virtual int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
                       int dir_fd,
                       const std::vector<PartPlacement>& parts,
                       const std::string& output_name,
                       std::optional<uint64_t> stride) const = 0;

  /* The shared data file for an upload running at this stride, if the
   * layout has one.  THE NAME CARRIES THE VALUE, which is what makes
   * establishment atomic:  creating the file and publishing the stride
   * are the same linkat, so a reader either finds the name and knows
   * the stride, or does not and knows there is none.  A fixed name
   * would need the value recorded separately, and a fact kept in two
   * places is a fact that can disagree -- which it did, between the
   * part record and the multipart cache, before this. */
  virtual std::optional<std::string> shared_name(uint64_t stride) const {
    return std::nullopt;
  }

  virtual const char* name() const = 0;
};

/* The layout nsfs writes today:  <bucket>/.multipart_<upload_id>/ holding
 * part-NNNNN, .meta and .assembled, assembled by copying each part into a
 * single output file.
 *
 * Named for what it does rather than for whose format it is.  "RGW" is
 * the prefix on half the classes in this tree, so it cannot carry the
 * distinction from NooBaa's layout that it was being asked to carry;
 * NooBaa's sibling can say NooBaa, because that word is specific.
 *
 * The zero padding is load-bearing for nothing -- consumers construct the
 * name from a number or parse it with std::stoul, and ordering comes from
 * a flat_map keyed on the number.  It is kept because this format is ours
 * and changing a name here would cost the only cheap proof that moving
 * these decisions behind an interface changed nothing. */
class PerPartMPUStrategy : public MPUStrategy {
public:
  std::string staging_dir_name(const std::string& meta) const override;

  bool names_staging_dir(std::string_view name) const override;
  std::string part_name(uint32_t part_num) const override;
  bool is_part_name(std::string_view name) const override;
  std::optional<uint32_t> part_number(std::string_view name) const override;

  std::optional<PartTarget> part_target(
      uint32_t part_num, std::optional<uint64_t> stride) const override;

  std::string head_name() const override;
  std::string meta_name() const override;
  std::string assembled_name() const override;

  const ReservedNames& reserved_names() const override;

  int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
               int dir_fd, const std::vector<PartPlacement>& parts,
               const std::string& output_name,
               std::optional<uint64_t> stride) const override;

  const char* name() const override { return "rgw"; }
};

/* The same layout, plus one data file the parts are written into.
 *
 * Derives from PerPartMPUStrategy because it IS that layout in every
 * respect but where a part's bytes land:  the staging directory, the
 * part records and the fixed names are unchanged, and only placement
 * and assembly differ.  Deriving says that;  a sibling class would not.
 *
 * Why it exists.  Assembling by copy is nearly free where the
 * filesystem shares extents and costs the whole object where it does
 * not -- GPFS answers EOPNOTSUPP at every granularity, measured on
 * Storage Scale 6.0.0.2.  Writing each part straight to its final
 * offset makes completing the upload a link instead.
 *
 * What a stride is.  The upload's uniform part size, which part 1
 * establishes at its completion and which never changes afterwards.
 * Part K's bytes go at (K-1) * stride.  A part arriving before a stride
 * exists, or one which would exceed its stride, takes its own file as
 * before -- so this layout degrades to its base class part by part
 * rather than failing.
 *
 * The stride is not a promise the client made.  S3 has no field for it
 * and the last part is normally shorter, which is why a part may write
 * less than its extent but never more. */
class StridedMPUStrategy : public PerPartMPUStrategy {
public:
  std::optional<PartTarget> part_target(
      uint32_t part_num, std::optional<uint64_t> stride) const override;

  const ReservedNames& reserved_names() const override;

  int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
	       int dir_fd, const std::vector<PartPlacement>& parts,
	       const std::string& output_name,
	       std::optional<uint64_t> stride) const override;

  std::optional<std::string> shared_name(uint64_t stride) const override;

  const char* name() const override { return "rgw-strided"; }
};

}}} // namespace rgw::sal::nsfs
