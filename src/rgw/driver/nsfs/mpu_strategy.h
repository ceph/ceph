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
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include "rgw_sal_fwd.h"
#include "rgw_acl.h"

#include "mpu_records.h"
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
   * reason part_number() is separate from part_name():  recognition and
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
  virtual bool is_staging_dir(std::string_view name) const = 0;

  /* What upload is staged in this directory.
   *
   * The question enumeration asks, and the one S3 cannot be served
   * without:  ListMultipartUploads orders by key and then upload id
   * and pages on both, so every upload's key has to be known before
   * a page can be decided.
   *
   * Where the key lives is the format's business and the cost
   * differs.  A layout which names the directory for the upload's
   * meta answers from `dname` alone.  NooBaa names it for a bare uuid
   * and keeps the key in a file inside, so theirs opens and reads
   * one, which is what their own listing does.
   *
   * `dname` is an entry of staging_root().  False means there is no
   * upload here -- an entry which is not one, or one whose metadata
   * cannot be read -- which a caller skips rather than fails on. */
  struct StagedUpload {
    std::string key;
    std::string upload_id;
    std::string bucket;

    /* What a format records about the upload beyond its identity.
     *
     * Read rather than invented:  NooBaa's completion path takes
     * content_type, content_encoding, xattr, storage_class and
     * lock_settings out of their meta file and puts them on the
     * finished object (`namespace_fs.js:2024`), so an upload we start
     * and their gateway finishes loses each of these if we do not
     * write it.  The reader half does not fill them in;  nothing asks
     * yet.
     *
     * Object lock is flat here and nested where a format writes it.
     * Keeping RGWObjectRetention out of this interface costs three
     * strings, and the strategies deal in what is on disk. */
    std::string content_type;
    std::string content_encoding;
    std::string storage_class;
    std::map<std::string, std::string> xattr;  /* user metadata, unprefixed */
    std::string retention_mode;                /* GOVERNANCE | COMPLIANCE */
    std::string retain_until;                  /* ISO 8601 */
    std::string legal_hold;                    /* ON | OFF */
  };

  virtual bool staged_upload(const DoutPrefixProvider* dpp, int root_fd,
			     std::string_view dname,
			     StagedUpload& out) const = 0;

  /* Who owns this upload, where the format can say.
   *
   * Asked only for the uploads a listing returns, not for every entry
   * examined -- it is a read, and S3 wants an owner in the response.
   *
   * False means the format keeps the answer with the upload's own
   * record, and the caller reads that as it always has.  Ours does.
   * NooBaa records no owner anywhere, so the staging directory's uid
   * is the answer, as it is for an object;  answering here is what
   * stops their uploads being dropped for want of a record of ours. */
  virtual bool upload_owner(const DoutPrefixProvider* dpp, int root_fd,
			    std::string_view dname, ACLOwner& out) const {
    return false;
  }

  /* What CreateMultipartUpload settled, and every later operation on
   * this upload must honour:  placement and storage class, the
   * object-lock family, the checksum algorithm.
   *
   * The third of the same family.  staged_upload() answers the
   * upload's identity and is asked of every entry a listing examines,
   * so it stays cheap;  upload_owner() and this are asked for one
   * upload and may open and read.
   *
   * Both inputs, for the same reason part_record() takes both:
   * `attrs` are the meta object's, already read through the bucket's
   * XattrStrategy, and ours is in there;  `root_fd` and `dname` are
   * for a format whose record that reader does not surface, and
   * NooBaa's is the content of a file rather than an attribute at
   * all.
   *
   * Typed rather than on-disk, as object_owner() is:  parsing an ISO
   * 8601 date or a retention mode is format knowledge, and leaving it
   * to the caller re-creates in the driver the thing these interfaces
   * exist to remove.
   *
   * A format fills what it holds and leaves the rest at its default.
   * NooBaa's document carries no placement -- a placement rule is an
   * RGW concept and inert on a filesystem -- and no checksum
   * algorithm, because their CreateMultipartUpload does not persist
   * one;  an upload of theirs therefore completes without a composite
   * checksum, which is what their own gateway does.
   *
   * `extra`, when given, takes what the create request settled that
   * RGW keeps as object attributes rather than in
   * multipart_upload_info:  the content type, the content encoding
   * and the user metadata.  Ours are already in `attrs`, because for
   * us they ARE attributes on the meta object -- so ours adds
   * nothing and theirs adds what their document holds.  Two shapes
   * of answer from one read, because RGW splits them that way and
   * not because the formats do.
   *
   * `owner`, likewise, where the format's record carries one -- ours
   * does, in the same blob.  upload_owner() is the same question
   * asked by the listing path, which has read no record and must not:
   * it answers for a format that can tell from the directory alone,
   * and returns false for one that cannot, and the caller then comes
   * here.  Two entry points because the two callers have different
   * things in hand, not because it is two questions. */
  virtual bool upload_info(const DoutPrefixProvider* dpp, int root_fd,
			   std::string_view dname, const Attrs& attrs,
			   multipart_upload_info& out,
			   Attrs* extra = nullptr,
			   ACLOwner* owner = nullptr) const = 0;

  /* The directory holding staging directories, relative to the bucket.
   *
   * "." where uploads stage directly under the bucket.  NooBaa puts
   * theirs two levels down, so a caller enumerating uploads has to ask
   * rather than assume the bucket directory.
   *
   * Resolved against a fd because the answer can depend on what is
   * there:  NooBaa's path contains a bucket id from their config store,
   * which this interface never reads.  Returns nullopt when the layout
   * has a root it cannot find, which is not an error -- a bucket with
   * no uploads has no staging root. */
  virtual std::optional<std::string> staging_root(
      const DoutPrefixProvider* dpp, int bucket_fd) const {
    return std::string{"."};
  }

  /* Where staging WOULD go, creating what is missing.
   *
   * A different question from staging_root(), which must not name a
   * directory that is not there -- a bucket with no uploads lists
   * empty rather than pointing an enumeration at nothing.  This one
   * is asked when an upload is about to be placed, and may create.
   *
   * For NooBaa that means the bucket temp directory, whose name
   * carries a bucket id from their config store.  An existing one is
   * used, always:  the id is then theirs, and an upload we start is
   * one their gateway can find.  One is created only where the tree
   * has none, because there is nothing to conflict with;  and more
   * than one is still refused, because which holds the uploads is not
   * decidable.
   *
   * The residual risk is narrow and named:  a tree with no temp
   * directory which we write to and NooBaa later serves ends up with
   * two, ours orphaned. */
  virtual std::optional<std::string> staging_root_for_write(
      const DoutPrefixProvider* dpp, int bucket_fd) const {
    return staging_root(dpp, bucket_fd);
  }

  virtual std::string part_name(uint32_t part_num) const = 0;
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

  /* Where a part has to move to, once its true length is known.
   *
   * part_target() is asked before the body has been read and answers
   * from the upload's stride, so a part which turns out to be a
   * different length is not necessarily where this layout can read it
   * from.  Ours can:  the stride's file holds every shared part at the
   * offset its record gives, and a short part simply does not fill its
   * slot.  NooBaa's file IS the size -- `parts-size-N` holds N-sized
   * parts at N * (num - 1) -- so a part which is not the stride is in
   * the wrong file, and their reader would open one that does not
   * exist.
   *
   * Nothing means the part stays where it was written, which is the
   * answer whenever `stored` equals the stride.  S3 requires every
   * part but the last to be equal, so at most one part per upload
   * moves. */
  virtual std::optional<PartTarget> relocation_target(
      uint32_t part_num, uint64_t stored,
      std::optional<uint64_t> stride) const = 0;

  /* What a part's record says about it.
   *
   * Read per format, because the record is the staging layout's and
   * not the object metadata's.  Ours is one ceph-encoded blob under a
   * single attribute;  NooBaa keeps size, offset and etag as three
   * plain attributes of their own.
   *
   * Both inputs are offered because the formats need different ones.
   * `attrs` are the logical attributes the caller has already read
   * through the bucket's XattrStrategy, which is where ours lives and
   * which carries the value decoding that strategy owns.  `dir_fd`
   * and `pname` are for a format whose record that reader does not
   * surface at all -- our reader drops `user.noobaa.*` as foreign, so
   * theirs opens the part itself.  A directory fd and a name rather
   * than an open file, because the caller stats parts without opening
   * them and a format which needs no open should not pay for one.
   *
   * False means this part has no readable record.  `size` may still
   * be unknown afterwards, in which case the caller falls back to the
   * file's length, as their own listing does. */
  struct PartRecord {
    uint64_t size{0};      /* accounted:  what the client sent */
    uint64_t stored{0};    /* bytes on disk */
    uint64_t offset{0};    /* where in the file they begin */
    bool shared{false};    /* other parts write into that file */
    std::string etag;
    ceph::real_time mtime;
    std::optional<rgw::cksum::Cksum> cksum;
  };

  virtual bool part_record(const DoutPrefixProvider* dpp, int dir_fd,
			   std::string_view pname, const Attrs& attrs,
			   PartRecord& out) const = 0;

  /* Record an upload, and record a part:  the write halves of
   * staged_upload() and part_record().
   *
   * Each format writes what its own reader reads.  Ours keeps both
   * records in one ceph-encoded attribute which the caller writes
   * through the bucket's XattrStrategy, so these do nothing and the
   * meta file stays empty.  NooBaa keeps the key in the meta file's
   * content and a part's size and offset in attributes of their own,
   * and neither is anything an XattrStrategy renders -- so theirs
   * write directly, on the same fd and name the reader opens.
   *
   * The caller writes its own attribute either way.  A base bucket
   * therefore carries both records:  theirs, which their gateway and
   * our listing read, and ours under `user.nsfs.`, which is inert to
   * them and is where the checksum lives.  S3 requires a checksum
   * agreed at CreateMultipartUpload to reach CompleteMultipartUpload,
   * and their file has nowhere to put one.
   *
   * `dir_fd` is the upload's staging directory. */
  virtual int write_staged_upload(const DoutPrefixProvider* dpp, int dir_fd,
				  const StagedUpload& su) const = 0;
  virtual int write_part_record(const DoutPrefixProvider* dpp, int dir_fd,
				std::string_view pname,
				const PartRecord& rec) const = 0;

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
  bool is_staging_dir(std::string_view name) const override;
  bool staged_upload(const DoutPrefixProvider* dpp, int root_fd,
		     std::string_view dname,
		     StagedUpload& out) const override;
  bool part_record(const DoutPrefixProvider* dpp, int dir_fd,
		   std::string_view pname, const Attrs& attrs,
		   PartRecord& out) const override;
  bool upload_info(const DoutPrefixProvider* dpp, int root_fd,
		   std::string_view dname, const Attrs& attrs,
		   multipart_upload_info& out,
		   Attrs* extra = nullptr,
		   ACLOwner* owner = nullptr) const override;
  int write_staged_upload(const DoutPrefixProvider* dpp, int dir_fd,
			  const StagedUpload& su) const override;
  int write_part_record(const DoutPrefixProvider* dpp, int dir_fd,
			std::string_view pname,
			const PartRecord& rec) const override;

  std::string part_name(uint32_t part_num) const override;
  std::optional<uint32_t> part_number(std::string_view name) const override;

  std::optional<PartTarget> part_target(
      uint32_t part_num, std::optional<uint64_t> stride) const override;
  std::optional<PartTarget> relocation_target(
      uint32_t part_num, uint64_t stored,
      std::optional<uint64_t> stride) const override;

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
  std::optional<PartTarget> relocation_target(
      uint32_t part_num, uint64_t stored,
      std::optional<uint64_t> stride) const override;

  const ReservedNames& reserved_names() const override;

  int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
	       int dir_fd, const std::vector<PartPlacement>& parts,
	       const std::string& output_name,
	       std::optional<uint64_t> stride) const override;

  std::optional<std::string> shared_name(uint64_t stride) const override;

  const char* name() const override { return "rgw-strided"; }
};

/* NooBaa's layout, which is what the base profile is.
 *
 *   <bucket>/.noobaa-nsfs_<bucket id>/multipart-uploads/<upload id>/
 *       create_object_upload    JSON, the create parameters;  key among them
 *       part-<n>                the part's record, size and offset in xattrs
 *       parts-size-<size>       the shared data file for parts of that size
 *       final                   the assembled object
 *
 * Read from noobaa-core at 68ca22d33 (2026-05-27):  `namespace_fs.js`
 * `_mpu_root_path`, `_mpu_path`, `_get_part_data_path`,
 * `_get_part_md_path`, `upload_multipart`, `complete_object_upload`.
 *
 * A part's offset is its OWN size times (n - 1), not an upload-wide
 * stride.  A part of a different length therefore addresses a
 * different file rather than a different offset in the same one, which
 * is why completing an upload whose last part is short copies that
 * part:  the body is linked and the tail is appended.
 *
 * `parts-size-<size>` is spelled exactly as ours.  The two readings
 * agree for every part but a short final one and disagree there, so
 * nothing may identify a layout by that name;  the staging directory's
 * location and its create_object_upload file are what distinguish
 * them. */
class NooBaaMPUStrategy : public MPUStrategy {
public:
  std::string staging_dir_name(const std::string& meta) const override;
  bool is_staging_dir(std::string_view name) const override;
  bool staged_upload(const DoutPrefixProvider* dpp, int root_fd,
		     std::string_view dname,
		     StagedUpload& out) const override;
  bool part_record(const DoutPrefixProvider* dpp, int dir_fd,
		   std::string_view pname, const Attrs& attrs,
		   PartRecord& out) const override;
  int write_staged_upload(const DoutPrefixProvider* dpp, int dir_fd,
			  const StagedUpload& su) const override;
  int write_part_record(const DoutPrefixProvider* dpp, int dir_fd,
			std::string_view pname,
			const PartRecord& rec) const override;
  bool upload_owner(const DoutPrefixProvider* dpp, int root_fd,
		    std::string_view dname, ACLOwner& out) const override;
  bool upload_info(const DoutPrefixProvider* dpp, int root_fd,
		   std::string_view dname, const Attrs& attrs,
		   multipart_upload_info& out,
		   Attrs* extra = nullptr,
		   ACLOwner* owner = nullptr) const override;
  std::optional<std::string> staging_root(const DoutPrefixProvider* dpp,
					  int bucket_fd) const override;
  std::optional<std::string> staging_root_for_write(
      const DoutPrefixProvider* dpp, int bucket_fd) const override;

  std::string part_name(uint32_t part_num) const override;
  std::optional<uint32_t> part_number(std::string_view name) const override;

  std::optional<PartTarget> part_target(
      uint32_t part_num, std::optional<uint64_t> stride) const override;
  std::optional<PartTarget> relocation_target(
      uint32_t part_num, uint64_t stored,
      std::optional<uint64_t> stride) const override;

  std::string meta_name() const override;
  std::string assembled_name() const override;
  const ReservedNames& reserved_names() const override;

  int assemble(const DoutPrefixProvider* dpp, FSStrategy* fs,
	       int dir_fd, const std::vector<PartPlacement>& parts,
	       const std::string& output_name,
	       std::optional<uint64_t> stride) const override;

  std::optional<std::string> shared_name(uint64_t stride) const override;

  const char* name() const override { return "noobaa"; }
};

}}} // namespace rgw::sal::nsfs
