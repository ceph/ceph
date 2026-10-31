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

#include "rgw_sal_filter.h"
#include "rgw_sal_store.h"
#include "rgw_quota.h"
#include "driver/posix/sync_policy.h"
#include "include/encoding.h"
#include <cstdint>
#include <memory>
#include "common/dout.h"
#include "../posix/bucket_cache.h"
#include "../posix/multipart_cache.h"
#include "../posix/posixDB.h"
#include "../posix/user_cache.h"
#include "../posix/fs_notification.h"
#include "../posix/qd2_pending.h"
#include "../posix/posix_io_uring.h"
#include "fs_strategy.h"
#include "mpu_records.h"
#include "mpu_strategy.h"
#include "xattr_strategy.h"
#include "path_strategy.h"
#include "access_strategy.h"
#include "bucket_state_strategy.h"
#include "identity_db.h"
#include "impersonate.h"
#include "bucket_profile.h"
#include "sync_fs_set.h"
#include "adopt/adoption_queue.h"

class RGWLC;

namespace rgw { namespace sal {

class NSFSDriver;
class NSFSBucket;
class NSFSObject;

/* What a mutation's slot is for (DECISIONS.md 2026-10-21, the gate).
 *
 * A bucket without `user.nsfs.extensions` has not been taken over, and
 * a client's write to it is rejected until adoption marks it.  Two
 * writers are not a client's:  adoption, which writes a bucket's state
 * before it marks it, and a copy's source, which is read and not
 * written but takes the slot so it waits out a rewrite. */
enum class MutationPurpose { client, adoption, copy_source };

namespace nsfs {

struct NooBaaStoreContents;
struct IdentityMapping;

/* The multipart cache's bucket field, which has to find the bucket again
 * from an eviction with nothing else in hand.  `<tenant>/<name>`, or the
 * name alone without a tenant;  a bucket name cannot contain '/'. */
inline std::string mp_cache_bucket(const std::string& tenant,
				   const std::string& name)
{
  return tenant.empty() ? name : (tenant + "/" + name);
}

/* {tenant, name} back from mp_cache_bucket(). */
inline std::pair<std::string, std::string>
mp_cache_bucket_split(const std::string& field)
{
  const auto slash = field.find('/');
  if (slash == std::string::npos) {
    return {std::string{}, field};
  }
  return {field.substr(0, slash), field.substr(slash + 1)};
}

/* What the upgrade is doing, at the moment it calls back.
 *
 * A bucket moving from base to a named profile rewrites every object
 * in the tree, and the callback is how a caller follows that:  for
 * progress and checkpointing in a deployment, and in a test for
 * sequencing a concurrent reader against the exact instant it wants.
 *
 * Three phases, because the interesting state is inside an item and
 * not between two of them.  `before` is the old format, which the read
 * fallback strategy serves.  `written` is the only genuinely new state
 * -- the target format is on disk and the old spelling has not gone --
 * and it is where asking the nsfs strategy first stops being incidental.
 * `pruned` is one spelling left, and the item is native.
 *
 * A non-zero return stops the conversion with the marker still set, so
 * the same hook that sequences a reader injects a failure and leaves a
 * tree for a restart to finish. */
struct ConvertEvent {
  enum class Kind { bucket, object, directory_object, version, upload,
                    part };
  enum class Phase { before, written, pruned };

  Kind kind;
  Phase phase;
  std::string key;         /* the object, or the upload's object;
                            * the bucket name when kind is bucket.
                            * For a version it is the object the
                            * version belongs to, with no suffix --
                            * version_id carries the rest. */
  std::string upload_id;   /* empty unless kind is upload or part */
  std::string version_id;  /* empty unless kind is version */
  uint32_t part{0};
  uint64_t index{0};       /* items reached so far, this run */
};

const char* to_string(ConvertEvent::Kind k);
const char* to_string(ConvertEvent::Phase p);

using convert_cb_t =
  const fu2::unique_function<int(const DoutPrefixProvider*,
				 const ConvertEvent&) const>;

/* What a conversion run did, whether or not it finished. */
struct ConvertProgress {
  uint64_t objects{0};
  uint64_t directory_objects{0};
  /* Counted apart from objects, because a resumed run has only these
   * numbers and its marker to report from:  folded together, a count
   * of 1540 cannot be reconciled against a bucket of 1200 keys, and a
   * half-finished sweep reads the same as a bucket with history. */
  uint64_t versions{0};
  uint64_t uploads{0};
  uint64_t parts{0};
  /* Symbolic links, which are not objects and are not converted (DECISIONS.md
   * 2026-10-23).  Counted by what each resolves to as NooBaa resolved it, its
   * real path against the bucket's, with read-search held so the count does
   * not depend on the gateway's uid, and so an operator sees what the gateway
   * no longer serves.  A link a filesystem error keeps from resolving is
   * counted dangling, and logged as an error. */
  struct Links {
    uint64_t file{0};       /* to a file in the bucket */
    uint64_t directory{0};  /* to a directory in the bucket */
    uint64_t outside{0};    /* out of the bucket */
    uint64_t dangling{0};   /* to nothing */
    uint64_t loop{0};       /* to itself, directly or through others */
    uint64_t total() const {
      return file + directory + outside + dangling + loop;
    }
  } links;
  bool complete{false};    /* nothing foreign left;  the marker is gone */
  /* the mask was written, so the bucket IS in the target profile.
   *
   * Separate from `complete` because they fail apart:  a run that
   * stops halfway has changed the profile and not finished the tree,
   * and the bucket is servable in exactly that state.  A caller that
   * reads only the return code cannot tell the two, and the two need
   * different things done about them -- nothing, and a resume. */
  bool profile_set{false};
};

using BucketCache = file::listing::BucketCache<NSFSDriver, NSFSBucket>;
using MultipartCache = file::listing::MultipartCache<>;

/* integration w/bucket listing cache */
using fill_cache_cb_t = file::listing::fill_cache_cb_t;

struct ObjectType {
  enum Type {
    UNKNOWN = 0,
    FILE = 1,
    DIRECTORY = 2,
    MULTIPART = 4,
  };
  uint32_t type{UNKNOWN};

  ObjectType &operator=(ObjectType::Type &&_t) {
    type = _t;
    return *this;
  };

  ObjectType() {}
  ObjectType(Type _t) : type(_t){}

  bool operator==(const ObjectType &t) const { return (type == t.type); }
  bool operator==(const ObjectType::Type &t) const { return (type == t); }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    ceph::decode(type, bl);
    DECODE_FINISH(bl);
  }
  friend inline std::ostream &operator<<(std::ostream &out,
                                         const ObjectType &t) {
    switch (t.type) {
    case UNKNOWN:
      out << "UNKNOWN";
      break;
    case FILE:
      out << "FILE";
      break;
    case DIRECTORY:
      out << "DIRECTORY";
      break;
    case MULTIPART:
      out << "MULTIPART";
      break;
    }
    return out;
  }
};
WRITE_CLASS_ENCODER(ObjectType);

class Directory;

/* Attribute write contract.
 *
 * REPLACE_ALL is the bucket contract:  bucket attrs are one map (a
 * single blob on rados), so the incoming set is the complete set and
 * anything absent from it is removed.  The bucket delete ops in
 * rgw_op.cc depend on this -- they erase from the map and call
 * put_info().
 *
 * MERGE is the object contract, matching RGWRados::set_attrs():  the
 * incoming set is added or overwritten, nothing else is touched, and
 * removals are named explicitly in rmattrs.
 */
enum class AttrWriteMode {
  REPLACE_ALL,
  MERGE,
};

class FSEnt {
protected:
  std::string fname;
  Directory* parent;
  int fd{-1};
  nsfs::FSIdentity identity;
  /* What decides once the filesystem denies `identity`;  null lets
   * no denial through.  See set_access_model(). */
  const nsfs::AccessStrategy* access_model{nullptr};
  bool read_only{false};
  bool need_fsync{false};
  bool exist{false};
  /* Zero-initialized so that get_stx() on an entry which has not been
   * stat()ed reads as "no information":  statx sets in stx_mask what
   * it filled in, and a zero mask is what a reader tests before
   * touching a field. */
  struct statx stx{};
  bool stat_done{false};
  CephContext* ctx;
  FSStrategy* fs_strategy;
  /* the multipart layout, carried the same way and for the same reason as
   * fs_strategy:  an MPDirectory has to name and enumerate parts, and is
   * built from its parent rather than from the driver */
  MPUStrategy* mpu_strategy{nullptr};
  /* selected by the bucket's recorded format;  carried here for the same
   * reason as the others -- an FSEnt reads and writes attributes and is
   * built from its parent */
  XattrStrategy* xattr_strategy{nullptr};
  PathStrategy* path_strategy{nullptr};
  /* the fallback strategies, null unless the bucket is
   * mid-upgrade;  carried and inherited for the same reason as the
   * others.  See NSFSBucket::xattr_fallback(). */
  XattrStrategy* xattr_fallback{nullptr};
  PathStrategy* path_fallback{nullptr};
  /* The names for this entry's own attributes, when they are not the
   * format's:  a bucket's directory carries the bucket's state, which is
   * RGW's in every format (DECISIONS.md 2026-10-13).  Not inherited --
   * the entries beneath are in the bucket's format -- and with no
   * fallback, since nothing else writes bucket state there. */
  XattrStrategy* own_xattr_strategy{nullptr};
  /* the union of what every strategy calls its own scaffolding, assembled
   * once by the driver.  Carried rather than asked for, because the
   * listing paths test it per directory entry. */
  const ReservedNames* reserved_names{nullptr};

public:
  static constexpr uint32_t FLAG_NONE =      0x0;
  static constexpr uint32_t FLAG_CURRENT =   0x2;
  static constexpr uint32_t FLAG_LIST_VERSIONS = 0x4;

  FSEnt(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat = nullptr);
  FSEnt(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx, FSStrategy* _strat = nullptr);
  FSEnt(const FSEnt& _ent) :
    fname(_ent.fname),
    parent(_ent.parent),
    exist(_ent.exist),
    stx(_ent.stx),
    stat_done(_ent.stat_done),
    ctx(_ent.ctx),
    fs_strategy(_ent.fs_strategy),
    mpu_strategy(_ent.mpu_strategy),
    xattr_strategy(_ent.xattr_strategy),
    path_strategy(_ent.path_strategy),
    xattr_fallback(_ent.xattr_fallback),
    path_fallback(_ent.path_fallback),
    own_xattr_strategy(_ent.own_xattr_strategy),
    reserved_names(_ent.reserved_names)
  { }

  virtual ~FSEnt() { }

  int get_fd() { return fd; };
  /* seeded on the root directory by the driver;  every child inherits it
   * through the constructors above */
  void set_mpu_strategy(MPUStrategy* s) { mpu_strategy = s; }
  MPUStrategy* get_mpu_strategy() const { return mpu_strategy; }
  void set_xattr_strategy(XattrStrategy* s) { xattr_strategy = s; }
  XattrStrategy* get_xattr_strategy() const { return xattr_strategy; }
  void set_path_strategy(PathStrategy* s) { path_strategy = s; }
  PathStrategy* get_path_strategy() const { return path_strategy; }
  void set_xattr_fallback(XattrStrategy* s) { xattr_fallback = s; }
  XattrStrategy* get_xattr_fallback() const { return xattr_fallback; }
  void set_path_fallback(PathStrategy* s) { path_fallback = s; }
  PathStrategy* get_path_fallback() const { return path_fallback; }
  void set_own_xattr_strategy(XattrStrategy* s) { own_xattr_strategy = s; }
  void set_reserved_names(const ReservedNames* r) { reserved_names = r; }
  const ReservedNames* get_reserved_names() const { return reserved_names; }
  void set_sync_on_close(bool sync) { need_fsync = sync; }
  std::string& get_name() { return fname; }
  Directory* get_parent() { return parent; }
  bool exists() { return exist; }
  struct statx& get_stx() { return stx; }
  virtual ObjectType get_type() { return ObjectType::UNKNOWN; };

  /* The identity this entry's descriptors are obtained under.
   *
   * Set rather than passed, so open() and create() keep their
   * signatures and their call sites.  An inactive identity means the
   * gateway's own credentials, which is every caller that has not
   * been told otherwise.  Who, not a personality id:  open() and
   * create() look the id up on the thread they run on.
   *
   * It applies to the *final component* only.  `resolve_path()`
   * opens each ancestor with `Directory::open()`, which takes no
   * identity, so a key directory's own permissions are never checked
   * against the identity.  NooBaa checks every component, because
   * it opens an object by its path under the account's
   * credentials;  closing that gap means passing this down the walk.
   *
   * There is no descriptor cache in the way:  `dir_chain` is built
   * per request and `Directory::open()` memoises `fd` on the entry,
   * which lasts one request. */
  void set_identity(const nsfs::FSIdentity& id) { identity = id; }
  const nsfs::FSIdentity& get_identity() const { return identity; }

  /* Where `identity` is denied:  a walk goes on as the gateway where
   * the model permits AccessClass::path, and a create, link, rename or
   * unlink is performed by the gateway where it permits
   * AccessClass::data (with_identity_or_raise()). */
  void set_access_model(const nsfs::AccessStrategy* m) { access_model = m; }
  const nsfs::AccessStrategy* get_access_model() const { return access_model; }

  /* Ask for no more access than the caller needs.
   *
   * Descriptors are opened O_RDWR by default, which cost nothing
   * while the gateway owned every file and could write all of
   * them.  Under impersonation a read is often the whole of what
   * the authenticated user is entitled to -- an object reachable through a
   * supplementary group and not writable at all -- and the
   * authorisation pass reads attributes with no identity at all,
   * holding only CAP_DAC_READ_SEARCH.  Both need the open to ask
   * for a read and stop there.
   *
   * Set rather than passed, like the identity above, so open()
   * keeps its signature and its call sites. */
  void set_read_only(bool ro) { read_only = ro; }

  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed = nullptr, bool temp_file = false) = 0;
  virtual int open(const DoutPrefixProvider *dpp) = 0;
  virtual int close() = 0;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false);
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) = 0;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  /* `use_fd` >= 0 reads from the caller's descriptor rather than the
   * entry's, for the same reason write_attrs() takes one:  it was
   * opened as the gateway and the caller holds the raise.  Reaching
   * this for *data* needs an access model that says so, and no
   * configured model does. */
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y,
		   int use_fd = -1) = 0;
  /* A descriptor for this entry opened as the **gateway**, owned by
   * the caller.
   *
   * Not the entry's `fd`, and deliberately:  a descriptor the gateway
   * obtained must not be reachable from the object, or a data path
   * finds it already open and serves bytes the filesystem denied
   * this identity -- measured 2026-10-16, and the reason this exists
   * rather than a flag on the entry.
   *
   * The caller closes it, and holds whatever raise it needs around
   * both this and the attribute calls it hands the descriptor to:
   * the open and the `user.*` syscalls are each tested on their own.
   *
   * Only the attribute paths want this.  A data path opens as the
   * authenticated user or not at all. */
  virtual int open_scratch_as_gateway(const DoutPrefixProvider* dpp,
				      bool read_only);

  /* `use_fd` >= 0 does the attribute work on that descriptor rather
   * than opening the entry, and with no identity -- it is the
   * caller's, obtained above, and the caller holds the raise.  The
   * default is what every caller has always had. */
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs,
			  AttrWriteMode mode = AttrWriteMode::REPLACE_ALL,
			  const std::vector<std::string>* rmattrs = nullptr,
			  int use_fd = -1);
  /* `unknown`, if given, receives every `user.noobaa.*` name the
   * reviewed releases do not write (X4's third layer);  a caller that
   * hands out the object refuses on one, and listing passes none */
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs,
			 int use_fd = -1,
			 std::vector<std::string>* unknown = nullptr);
  /* src_as_gateway:  the access model permits the gateway to read a
   * source the filesystem denies the authenticated user.  Resolved by the
   * caller, which knows the bucket;  see clone_file_by_copy(). */
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name, const CopyHints& hints, bool src_as_gateway = false) = 0;
  /* excl:  fail with -EEXIST rather than replace, for If-None-Match: * */
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y,
			     std::string target_fname, bool excl = false,
			     bool as_gateway = false) = 0;
  virtual std::unique_ptr<FSEnt> clone_base() = 0;
  /* Build this entry's listing row.  Only Directory::fill_cache()
   * composes keys from a path prefix as it descends;  every other caller
   * already knows the object's key and should pass it, rather than
   * borrowing a prefix-relative walk to produce one row.
   *
   * `bucket_owner` is the owner of an object that records none --
   * NSFSBucket::default_object_owner(). */
  int make_dir_entry(const DoutPrefixProvider* dpp, optional_yield y,
		     const rgw_obj_key& key, uint32_t flags,
		     const ACLOwner& bucket_owner,
		     rgw_bucket_dir_entry& bde);
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const ACLOwner& bucket_owner, const std::string& path_prefix = "");
  virtual std::string get_cur_version() { return ""; };
};

class File : public FSEnt {
protected:
  bool direct_io{false};

public:
  File(std::string _name, Directory* _parent, CephContext* _ctx) : FSEnt(_name, _parent, _ctx)
    {}
  File(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : FSEnt(_name, _parent, _stx, _ctx)
    {}
  File(const File& _f) : FSEnt(_f) {}
  virtual ~File() { close(); }

  virtual uint64_t get_size() { return stx.stx_size; }
  virtual ObjectType get_type() override { return ObjectType::FILE; };


  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int close() override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y,
		   int use_fd = -1) override;
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name, const CopyHints& hints, bool src_as_gateway = false) override;
  virtual int rename(const DoutPrefixProvider* dpp, optional_yield y, Directory* dst_dir, const std::string& dst_name);
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y,
			     std::string target_fname, bool excl = false,
			     bool as_gateway = false) override;
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<File>(*this);
  }
  std::unique_ptr<File> clone() {
    return std::make_unique<File>(*this);
  }
};

class Directory : public FSEnt {
public:
  Directory(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat = nullptr) : FSEnt(_name, _parent, _ctx, _strat)
    {}
  Directory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : FSEnt(_name, _parent, _stx, _ctx)
    {}
  Directory(const Directory& _d) : FSEnt(_d) {}
  virtual ~Directory() { close(); }

  virtual ObjectType get_type() override { return ObjectType::DIRECTORY; };

  virtual bool file_exists(std::string& name);

  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int close() override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  template <typename F>
    int for_each(const DoutPrefixProvider* dpp, const F& func);
  virtual int rename(const DoutPrefixProvider* dpp, optional_yield y, Directory* dst_dir, std::string dst_name);
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y,
		   int use_fd = -1) override;
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<Directory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() {
    return std::make_unique<Directory>(*this);
  }
  std::unique_ptr<Directory> clone() {
    return std::make_unique<Directory>(*this);
  }
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name, const CopyHints& hints, bool src_as_gateway = false) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y,
			     std::string target_fname, bool excl = false,
			     bool as_gateway = false) override;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const ACLOwner& bucket_owner, const std::string& path_prefix = "") override;

  int get_ent(const DoutPrefixProvider *dpp, optional_yield y, const std::string& name, const std::string& version, std::unique_ptr<FSEnt>& ent);

  /* Is this directory itself an object, in a format that marks the
   * directory rather than relying on its sentinel?  Asks the bucket's
   * own path strategy and then the fallback.  The nsfs strategy returns
   * false without a syscall and the fallback is null except mid-
   * conversion, so a bucket in the nsfs format pays nothing here. */
  bool directory_object(const DoutPrefixProvider* dpp,
			PathStrategy::DirectoryObject& out);
};

class MPDirectory : public Directory {
  std::string tmpname;
protected:
  std::map<std::string, int64_t> parts;
  std::unique_ptr<FSEnt> cur_read_part;

public:
  MPDirectory(std::string _name, Directory* _parent, CephContext* _ctx) : Directory(_name, _parent, _ctx)
    {}
  MPDirectory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : Directory(_name, _parent, _stx, _ctx)
    {}
  MPDirectory(const MPDirectory& _d) :
    Directory(_d),
    parts(_d.parts)
    { if (_d.cur_read_part) cur_read_part = _d.cur_read_part->clone_base(); }
  virtual ~MPDirectory() { close(); }

  virtual ObjectType get_type() override { return ObjectType::MULTIPART; };
  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed = nullptr, bool temp_file = false) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y,
		   int use_fd = -1) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y,
			     std::string target_fname, bool excl = false,
			     bool as_gateway = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  std::unique_ptr<File> get_part_file(int partnum);
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() override {
    return std::make_unique<MPDirectory>(*this);
  }
  std::unique_ptr<MPDirectory> clone() {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const ACLOwner& bucket_owner, const std::string& path_prefix = "") override;
};

/* A directory object in NooBaa's format.
 *
 * NooBaa marks the directory itself:  user.noobaa.dir_content holds the
 * object's size, the object's attributes are the directory's, and the
 * bytes are in the sentinel only when there are any
 * (PathStrategy::directory_object()).  So one object is spread over two
 * entries, and this keeps that rule in one place behind the interface
 * every caller already uses (DECISIONS.md 2026-10-13).
 *
 * It reports what conversion will leave, because a client must not see
 * a bucket being converted differ from the bucket it becomes
 * (DECISIONS.md 2026-10-13):
 *
 *   - stat is the sentinel's when the object has content, and the
 *     directory's when it is empty, with the size from the marker
 *   - attributes are read from the directory, without the `user.nsfs.`
 *     names:  on the bucket root those are the bucket's own state, and
 *     nothing else in NooBaa's format carries them
 *   - data is the sentinel's, or nothing
 *
 * `parent` is the directory, and `fname` the sentinel's name.  Read
 * only:  a write to such an object has to convert it, which this does
 * not do, so the mutators reject it. */
class NooBaaDirectoryObject : public FSEnt {
  PathStrategy::DirectoryObject dobj;
  /* the sentinel, present only when the object has content */
  std::unique_ptr<File> content;

public:
  NooBaaDirectoryObject(const std::string& sentinel, Directory* dir,
			const PathStrategy::DirectoryObject& _dobj,
			CephContext* _ctx);
  NooBaaDirectoryObject(const NooBaaDirectoryObject& o);
  virtual ~NooBaaDirectoryObject() { close(); }

  virtual ObjectType get_type() override { return ObjectType::FILE; };

  virtual int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int close() override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y,
		   int use_fd = -1) override;
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs,
			  AttrWriteMode mode = AttrWriteMode::REPLACE_ALL,
			  const std::vector<std::string>* rmattrs = nullptr,
			  int use_fd = -1) override;
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs,
			 int use_fd = -1,
			 std::vector<std::string>* unknown = nullptr) override;
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name, const CopyHints& hints, bool src_as_gateway = false) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y,
			     std::string target_fname, bool excl = false,
			     bool as_gateway = false) override;
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<NooBaaDirectoryObject>(*this);
  }
};


/* Walk `key_path` from `root`, making missing directories when
 * `create_dirs`.
 *
 * As `id`:  each directory is opened as it, and one the walk makes is
 * its, at dir_create_mode().  An inactive `id` is the gateway.  When
 * `id` is denied on the way, `model` decides
 * (`permits(AccessClass::path)`):  the denial stands, or the walk goes
 * on as the gateway.  A null `model` lets no denial through, which is
 * what a gateway walk needs, having none. */
int resolve_path(const DoutPrefixProvider* dpp,
                 Directory* root,
                 const std::string& key_path,
                 bool create_dirs,
                 CephContext* cct,
                 std::vector<std::unique_ptr<Directory>>& dir_chain,
                 Directory*& leaf_dir,
                 std::string& leaf_name,
                 const FSIdentity& id,
                 const AccessStrategy* model);

/* Resolve a client's S3 key to its path:  the one way a key reaches the
 * tree.  `key_path` is the key's on-disk name (object_name()).
 *
 * A key with a component the driver reserves -- `.folder`, `.versions`
 * -- is rejected here, for every operation:  such a name is schema, not
 * an object (DECISIONS.md 2026-10-12).  A key of slashes alone is not
 * rejected:  it is the root object, stored in the root's `.folder`
 * (DECISIONS.md 2026-10-13).  A lookup
 * (`create_dirs` false) returns -ENOENT, so a read is NoSuchKey and a
 * delete deletes nothing;  a create returns -ERR_INVALID_OBJECT_NAME.
 * Keys in a namespace of RGW's own are not clients' names and pass, as
 * do keys in an internal bucket (`internal_bucket`, NSFSBucket::
 * is_internal()):  an upload's own directory holds `.meta`.
 *
 * resolve_path() stays for paths that are not keys:  crash recovery,
 * conversion, the listing walk. */
int resolve_key_path(const DoutPrefixProvider* dpp,
                     Directory* root,
                     const rgw_obj_key& key,
                     const std::string& key_path,
                     const ReservedNames& rn,
                     bool internal_bucket,
                     bool create_dirs,
                     CephContext* cct,
                     std::vector<std::unique_ptr<Directory>>& dir_chain,
                     Directory*& leaf_dir,
                     std::string& leaf_name,
                     const FSIdentity& id,
                     const AccessStrategy* model);

} // namespace nsfs

class NSFSZoneGroup : public StoreZoneGroup {
  NSFSDriver* store;
  std::unique_ptr<RGWZoneGroup> group;
  std::string empty;

public:
  NSFSZoneGroup(NSFSDriver* _store, std::unique_ptr<RGWZoneGroup> _group) : store(_store), group(std::move(_group)) {}
  virtual ~NSFSZoneGroup() = default;

  virtual const std::string& get_id() const override { return group->get_id(); };
  virtual const std::string& get_name() const override { return group->get_name(); };
  virtual int equals(const std::string& other_zonegroup) const override {
    return group->equals(other_zonegroup);
  };
  virtual bool placement_target_exists(std::string& target) const override;
  virtual bool is_master_zonegroup() const override {
    return group->is_master_zonegroup();
  };
  virtual const std::string& get_api_name() const override { return group->api_name; };
  virtual void get_placement_target_names(std::set<std::string>& names) const override;
  virtual const std::string& get_default_placement_name() const override {
    return group->default_placement.name; };
  virtual int get_hostnames(std::list<std::string>& names) const override {
    names = group->hostnames;
    return 0;
  };
  virtual int get_s3website_hostnames(std::list<std::string>& names) const override {
    names = group->hostnames_s3website;
    return 0;
  };
  virtual int get_zone_count() const override {
    /* currently only 1 zone supported */
    return 1;
  }
  virtual int get_placement_tier(const rgw_placement_rule& rule,
				 std::unique_ptr<PlacementTier>* tier) {
    return -1;
  }
  virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone) override {
    return -1;
  }
  virtual int get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone) override {
    return -1;
  }
  virtual int list_zones(std::list<std::string>& zone_ids) override {
    zone_ids.clear();
    return 0;
  }
  virtual std::unique_ptr<ZoneGroup> clone() override {
    std::unique_ptr<RGWZoneGroup>zg = std::make_unique<RGWZoneGroup>(*group.get());
    return std::make_unique<NSFSZoneGroup>(store, std::move(zg));
  }
};

class NSFSZone : public StoreZone {
protected:
  NSFSDriver* store;
  std::unique_ptr<RGWRealm> realm;
  std::unique_ptr<NSFSZoneGroup> zonegroup;
  std::unique_ptr<RGWZone> zone_public_config; /* external zone params, e.g., entrypoints, log flags, etc. */
  std::unique_ptr<RGWZoneParams> zone_params; /* internal zone params, e.g., rados pools */
  std::unique_ptr<RGWPeriod> current_period;

public:
  NSFSZone(NSFSDriver* _store) : store(_store) {
    realm = std::make_unique<RGWRealm>();
    std::unique_ptr<RGWZoneGroup> rzg = std::make_unique<RGWZoneGroup>("default", "default");
    rzg->api_name = "default";
    rzg->is_master = true;
    zonegroup = std::make_unique<NSFSZoneGroup>(store, std::move(rzg));
    zone_public_config = std::make_unique<RGWZone>();
    zone_params = std::make_unique<RGWZoneParams>();
    current_period = std::make_unique<RGWPeriod>();

    // XXX: only default and STANDARD supported for now
    RGWZonePlacementInfo info;
    RGWZoneStorageClasses sc;
    sc.set_storage_class("STANDARD", nullptr, nullptr);
    info.storage_classes = sc;
    zone_params->placement_pools["default"] = info;
  }
  ~NSFSZone() = default;

  virtual std::unique_ptr<Zone> clone() override {
    return std::make_unique<NSFSZone>(store);
  }
  virtual ZoneGroup& get_zonegroup() override;
  const RGWZoneParams& get_rgw_params();
  virtual const std::string& get_id() override;
  virtual const std::string& get_name() const override;
  virtual bool is_writeable() override;
  virtual bool get_redirect_endpoint(std::string* endpoint) override;
  virtual const std::string& get_current_period_id() override;
  virtual const RGWAccessKey& get_system_key() override;
  virtual const std::string& get_realm_name() override;
  virtual const std::string& get_realm_id() override;
  virtual const std::string_view get_tier_type() override { return "rgw"; }
  virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() override;
};

class NSFSLuaManager : public StoreLuaManager {
  NSFSDriver* store;

public:
  NSFSLuaManager(NSFSDriver* _s) : store(_s)
  { }
  virtual ~NSFSLuaManager() = default;

  virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override;
  virtual std::tuple<rgw::lua::LuaCodeType, int> get_script_or_bytecode(const DoutPrefixProvider* dpp, optional_yield y,
                                                                        const std::string& key) override;
  virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override;
  virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override;
  virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
  virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
  virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages) override;
  virtual int reload_packages(const DoutPrefixProvider* dpp, optional_yield y) override;
};

class NSFSDriver : public StoreDriver {
  /* the `unmarked-bucket` hint, the only maker of an UnmarkPasskey */
  friend class UnmarkPasskey;
  int hint_unmarked_bucket(const DoutPrefixProvider* dpp,
			   const std::map<std::string, std::string>& params,
			   std::map<std::string, std::string>* out);
  /* buckets that hint has unmarked;  in the features map, 0 in service */
  std::atomic<uint64_t> hint_unmarked_buckets{0};

protected:
  CephContext *cct;
  /* The executor AppMain serves requests on, for work the driver spawns
   * itself.  Null only in unit tests that construct the driver directly. */
  boost::asio::io_context* io_context;
  /* The prefix for work that belongs to no request:  an FSIO handle is
   * shared, and its destructor runs wherever the last reference drops,
   * as late as an RGWFileHandle's reclaim.  It reads `cct` at each
   * call, so it follows set_context(). */
  class DriverPrefix : public DoutPrefixProvider {
    const NSFSDriver& driver;
  public:
    explicit DriverPrefix(const NSFSDriver& driver) : driver(driver) {}
    CephContext* get_cct() const override { return driver.cct; }
    unsigned get_subsys() const override { return ceph_subsys_rgw; }
    std::ostream& gen_prefix(std::ostream& out) const override {
      return out << "nsfs driver: ";
    }
  } driver_prefix{*this};
  /* the driver's database.  An IdentityDB rather than a plain
   * POSIXUserDB:  same file, same connection, one extra table for the
   * Scale data an identity carries. */
  std::unique_ptr<nsfs::IdentityDB> userDB;
  NSFSZone zone;
  std::unique_ptr<nsfs::BucketCache> bucket_cache;
  std::unique_ptr<nsfs::MultipartCache> multipart_cache;

  /* Buckets whose mutations are held off, and how many are still
   * running in each.
   *
   * An upgrade rewrites a tree, and a write arriving in the middle of
   * one object's conversion can be lost:  the converter read the
   * attributes before the write and writes them back after it.  So
   * the upgrade closes the gate, waits for what is already running to
   * finish -- every operation terminates, which is what makes the
   * wait bounded -- and converts.
   *
   * Reads are not gated.  A half-converted tree is readable through
   * the fallback strategies, which is why they exist, and blocking
   * reads for
   * the length of a conversion is the cost we set out to avoid.
   *
   * Per process.  A gate one gateway closes is invisible to the one
   * beside it until the invalidate upcall exists;  today that is not a
   * gap in practice, because the per-instance cache already prevents
   * two of them serving one root, but it is a gap in principle and
   * the upcall is what closes it. */
  ceph::mutex gate_lock = ceph::make_mutex("NSFSDriver::gate_lock");
  ceph::condition_variable gate_cond;
  struct BucketGate {
    bool closed{false};
    uint64_t inflight{0};
  };
  std::map<std::string, BucketGate, std::less<>> bucket_gates;
  /* the two waits;  see the constants they are seeded from */
  std::chrono::milliseconds mutation_gate_wait{5000};
  std::chrono::milliseconds gate_drain_wait{60000};
  /* Written by an adoption command on its request's thread and by the
   * queue's drain on its own, and read by every request that resolves a
   * bucket, so the record is held under a lock and whether there is
   * one is an atomic. */
  mutable std::mutex completion_mtx;
  std::optional<nsfs::AdoptionCompletion> completion;
  std::atomic<bool> completed{false};
  /* One-way, since nothing removes the record.  The first record seen
   * ends adoption's in-memory state. */
  void set_completion(nsfs::AdoptionCompletion c);
  /* `automatic` adoption while serving;  null in any other mode, in a
   * process with no background tasks, and once adoption is complete at
   * startup.  Published whole, since the `adoption` hint can start it
   * while requests read it;  the owner keeps it until the driver goes. */
  std::atomic<nsfs::AdoptionQueue*> adoption_queue{nullptr};
  std::unique_ptr<nsfs::AdoptionQueue> adoption_queue_owner;
  std::mutex adoption_queue_start_mtx;
  /* under adoption_queue_start_mtx:  set by shutdown(), after which
   * nothing starts the queue */
  bool adoption_queue_shut{false};
  /* the quantum, in ms;  set at runtime by the `adoption` hint's
   * `wait_ms`, while requests read it */
  std::atomic<uint64_t> adoption_wait_ms{1000};
  /* Buckets whose adoption failed, with the error, in either mode:  not
   * served until an operator acts -- adopts by command, abandons, or
   * restarts the gateway, which forgets them (DECISIONS.md 2026-10-29). */
  std::mutex adoption_failures_mtx;
  std::map<std::string, int> adoption_failures;
  /* under the relaxed sync policy only;  see init_sync_policy() */
  std::unique_ptr<nsfs::SyncFsSet> sync_set;
  UserCache user_cache;
  std::unique_ptr<nsfs::FSStrategy> fs_strategy;
  std::unique_ptr<nsfs::MPUStrategy> mpu_strategy;
  std::unique_ptr<nsfs::XattrStrategy> xattr_strategy;
  std::unique_ptr<nsfs::PathStrategy> path_strategy;
  /* NooBaa's three.  Held whether or not a bucket is in NooBaa's format,
   * because which format a bucket is in is not known until it is
   * loaded, and constructing a strategy costs nothing. */
  std::unique_ptr<nsfs::BucketStateStrategy> bucket_state_strategy;
  /* the access model, beside the format strategies and selected the
   * same way -- once, at startup, from configuration.
   *
   * `live` is what every caller reads, and the `access-model` driver
   * hint can point it at another one for the length of a test:  an
   * invariant nothing can break on demand is an invariant nothing
   * has tested.  The targets are statics, so the swap is a pointer
   * and no reader can be left holding a freed strategy. */
  std::unique_ptr<nsfs::AccessStrategy> access_strategy;
  std::atomic<const nsfs::AccessStrategy*> access_strategy_live{nullptr};
  /* How many operations the access model let the gateway perform
   * after the filesystem denied the authenticated user, by class.
   *
   * Reported by the `access-model` hint.  Without it a test that
   * sees a read succeed cannot tell *why*:  because the model
   * permitted it, or because the filesystem never objected and the
   * fixture is wrong.  The counts make a success attributable. */

  /* When a NooBaa tree is adopted (DECISIONS.md 2026-10-11).
   *
   * Meaningful only when a NooBaa configuration store is configured:
   * with no store there is nothing to adopt, so the mode is inert and
   * an ordinary nsfs deployment behaves as it always has.
   *
   * `Command`, the default, adopts nothing unless asked through Admin
   * Ops:  the `adoption` driver hint imports the store's identities or
   * adopts a bucket, and so does changing a bucket's profile out of
   * base.  Nothing on disk changes until then.  `Automatic` imports
   * the identities and adopts every bucket at startup. */
  enum class AdoptionMode { Command, Automatic };
  AdoptionMode adoption_mode{AdoptionMode::Command};

  /* null unless rgw_nsfs_noobaa_config_root names a store */
  std::unique_ptr<nsfs::BucketStateStrategy> nb_bucket_state_strategy;
  std::unique_ptr<nsfs::XattrStrategy> nb_xattr_strategy;
  std::unique_ptr<nsfs::PathStrategy> nb_path_strategy;
  std::unique_ptr<nsfs::MPUStrategy> nb_mpu_strategy;
  /* what a bucket's extensions marker resolves to;  both resolve to the
   * same strategy instances until S5 supplies the noobaa ones */
  /* The format shared and strong both use:  NooBaa's layout with the nsfs
   * metadata representation, which is what EXT_RGW_META declares. */
  nsfs::Format rgw_meta_format;
  /* What base is:  NooBaa's, entire. */
  nsfs::Format noobaa_format;
  nsfs::BucketProfile base_profile;
  nsfs::BucketProfile shared_profile;
  nsfs::BucketProfile extended_profile;
  nsfs::ReservedNames reserved_names;
  /* rgw_nsfs_base_path.  Read only through default_bucket_location()
   * and fs_reference_path(), which name its two purposes:  nothing is a
   * bucket because of where it sits, so it is never a bucket's parent
   * (DECISIONS.md 2026-10-13). */
  std::string base_path_;
  /* a descriptor on it, for the filesystem-wide facilities */
  int base_fd{-1};
  RGWSyncModuleInstanceRef sync_module;
  RGWQuotaHandler* quota_handler{nullptr};
  RGWLC* lc{nullptr};
  bool use_lc_thread{false};
  bool background_tasks{false};
  std::mutex unadopted_mapping_mtx;
  std::shared_ptr<const nsfs::IdentityMapping> unadopted_mapping;
  /* driver hint state (test only)--see driver_hint() */
  bool inject_fork_race{false};

  /* Added to the accounted size before the multipart writer compares it
   * against the bytes it actually stored.
   *
   * The strided staging layout does not establish a stride when those
   * two disagree, because a disagreement means a filter between the op
   * layer and the writer changed the byte count and the client's length
   * cannot place anything.  Compression and AEAD are the real causes,
   * and NEITHER IS REACHABLE ON NSFS:  NSFSZone hardcodes its one
   * storage class with no compression type, so the suppressing branch
   * has no way to be taken in a test.  A control whose negative case
   * cannot occur proves nothing, so this makes it occur. */
  /* Scoped to one upload, not global.
   *
   * A multipart upload is concurrent by nature and so is the test
   * suite:  a delta armed for the whole driver perturbs every upload
   * running at the time, which is how one test's negative case
   * silently suppressed another test's stride.  The unit the control
   * acts on is the upload -- the condition it forces is evaluated once
   * per upload, at part 1's completion -- so that is the unit it is
   * keyed by.  A bucket would isolate test from test but still not let
   * one test arm one upload and not another. */
  std::atomic<uint64_t> mpu_diverts{0};
  std::map<std::string, int64_t> inject_delta_by_upload;
  ceph::mutex inject_delta_lock =
      ceph::make_mutex("NSFSDriver::inject_delta_lock");
  bool inject_skip_reclone{false};
  /* write the non-md5 etag the way the defect did -- by the calling
   * thread, with the result discarded -- so a suite can watch the etag
   * the client is given diverge from the one recorded on disk, rather
   * than deleting the fix and rebuilding to prove the test can fail */
  bool inject_etag_as_gateway{false};
  /* read a denial in a delete the way the defect did -- as an absent
   * object -- so a test can watch a denied delete answer success, or
   * -EINVAL, rather than deleting the fix to prove it can fail.
   * Atomic:  a test thread arms it while another deletes */
  std::atomic<bool> inject_denial_as_absence{false};
  /* the user whose credentials not resolving NSFSReadOp::prepare(),
   * get_attr(), check_owner_access() and create() pass over, as they did
   * before work-plan stage N-F2, so a test can watch a metadata read served,
   * or a bucket placed or created, rather than deleting the fix.  By user,
   * since the suite runs in parallel;  empty is off */
  ceph::mutex unresolved_as_gateway_lock =
    ceph::make_mutex("NSFSDriver::unresolved_as_gateway_lock");
  std::string unresolved_as_gateway_user;
  /* -1 disables;  otherwise fail a rename after this many version entries
   * have moved, so the rollback path is exercised rather than assumed */
  int inject_rename_fail_after{-1};
  /* where an injected rename gives up *without* cleaning up, standing in for
   * a crash:  0 none, 1 after inject_rename_fail_after versions have moved,
   * 2 after the leaf has landed.  Either leaves the intent record behind,
   * which is the only state recovery can be tested against */
  int inject_rename_abandon{0};
  /* fail a shadow build, so the cleanup paths run rather than being
   * reasoned about:  "attrs" fails applying the create spec, "link" fails
   * the linkat which makes the shadow visible */
  bool inject_fsio_attrs_fail{false};
  bool inject_fsio_link_fail{false};
  /* skip the temp cleanup, so a test can assert that its detector sees a
   * leak as well as that the code does not produce one.  Same idea as
   * inject_skip_reclone:  the negative case belongs in the test rather than
   * in a hand-edited build */
  bool inject_fsio_skip_cleanup{false};
  /* make conversion behave as a daemon without CAP_CHOWN, so a test can
   * assert that it fails rather than creating a file whose owner is
   * wrong.  The capability is not droppable in a test process -- a
   * thread cannot regain it -- so the condition has to be injected */
  bool inject_no_chown{false};
  /* fail a client's bucket creation after its directory is made, so a
   * test can assert the rollback:  nothing in an ordinary setup makes
   * the steps after mkdir fail */
  bool inject_create_failure{false};
  /* fail an object's attribute read under get_obj_attrs() after its
   * open has succeeded, for objects in these buckets, so a test can
   * assert what a delete does when the object lock cannot be read.
   * Keyed by bucket, as inject_delta_by_upload is by upload:  armed
   * for the driver it would fail every lock test running at the
   * time */
  std::set<std::string> inject_attr_read_fail_buckets;
  std::atomic<bool> inject_attr_read_fail_armed{false};
  std::atomic<uint64_t> attr_read_fails_injected{0};
  ceph::mutex inject_attr_read_fail_lock =
      ceph::make_mutex("NSFSDriver::inject_attr_read_fail_lock");
  /* fail, with EIO, the attribute writes a copy, a delete marker and a
   * multipart completion make after the object exists, in these
   * buckets, so a test can assert each returns the failure rather
   * than discarding it.  Keyed by bucket, as the read failure is */
  std::set<std::string> inject_attr_write_fail_buckets;
  std::atomic<bool> inject_attr_write_fail_armed{false};
  std::atomic<uint64_t> attr_write_fails_injected{0};
  ceph::mutex inject_attr_write_fail_lock =
      ceph::make_mutex("NSFSDriver::inject_attr_write_fail_lock");
  /* lift the gate on a bucket not taken over, for these buckets, so a
   * test can show a rejection is the gate's:  the same write succeeds
   * with it lifted.  Keyed by bucket, as the attribute-read injection
   * is.  The count is every write the gate rejected. */
  std::set<std::string> inject_lift_adoption_gate_buckets;
  std::atomic<bool> inject_lift_adoption_gate_armed{false};
  std::atomic<uint64_t> unadopted_writes_rejected{0};
  ceph::mutex inject_lift_adoption_gate_lock =
      ceph::make_mutex("NSFSDriver::inject_lift_adoption_gate_lock");
  /* hold these buckets' adoption before it begins, so a test can keep
   * one not adopted while the queue runs:  a worker that takes a held
   * bucket waits until it is released or the gateway stops.  `waiting`
   * is the held buckets a worker has taken. */
  std::set<std::string> inject_hold_adoption_buckets;
  std::set<std::string> inject_hold_adoption_waiting;
  ceph::mutex inject_hold_adoption_lock =
      ceph::make_mutex("NSFSDriver::inject_hold_adoption_lock");
  ceph::condition_variable inject_hold_adoption_cond;

  /* Which POSIX identity a converted consumer chose for one named
   * object, so a test can assert the uid and not the status --  (n)
   * phase D's validation clause.  Phase C plumbed the authenticated
   * user and the compiler catches a missing identity, not a wrong
   * one;  this records the choice so a wrong one is a test failure
   * rather than a silent inversion.
   *
   * Selected by user, not global.  A global slot would hold
   * whatever ran last, and the suite runs in parallel -- see the
   * buffered-bytes counter, which is global and has no such
   * protection.
   *
   * By user rather than by object because the question is
   * whether *this caller's* operations ran as it, which is not a
   * per-object question:  a test may touch several objects, and a
   * bucket-level attribute write names no key at all.  A selected
   * user is also its own switch -- empty means record nothing -- so
   * there is no second flag to leave set. */
  ceph::mutex ran_as_lock = ceph::make_mutex("NSFSDriver::ran_as_lock");
  std::string ran_as_who;
  std::vector<std::pair<std::string, uid_t>> ran_as_seen;

  /* The `inject-buffered-copy` hint, by user.  Armed with `user=`,
   * a bundle applies to that user's copies only;  armed without,
   * the daemon's applies to every user with no bundle of its own,
   * and to the copies with no authenticated user at all.  A bundle is
   * kept for the driver's life once made, one per user a test has
   * armed, so the counter a CopyHints points at never dangles. */
  struct CopyBundle {
    std::atomic<bool> force_buffered{false};
    std::atomic<uint64_t> buffered_bytes{0};
  };
  CopyBundle daemon_copy_bundle;
  ceph::mutex copy_bundles_lock =
    ceph::make_mutex("NSFSDriver::copy_bundles_lock");
  std::map<std::string, CopyBundle> copy_bundles;
  std::atomic<bool> any_copy_bundle{false};

public:
  NSFSDriver(CephContext *_cct,
	     boost::asio::io_context* _io_context = nullptr)
    : StoreDriver(), cct(_cct), io_context(_io_context), zone(this)
  {
    const static std::string tenant = "default_ns";
    const auto& db_path = g_conf().get_val<std::string>("rgw_nsfs_userdb_dir");
    const auto& db_name = g_conf().get_val<std::string>("dbstore_db_name_prefix") + "-" + tenant;
    auto db_full_path = std::filesystem::path(db_path) / db_name;

    userDB = std::make_unique<nsfs::IdentityDB>(db_full_path.string(), cct);
  }
  /* The multipart cache goes before anything it calls back into.
   *
   * A writeback entry's eviction runs `stabilize_fn`, which captures
   * the driver and asks it for the root fd.  The cache is a member, so
   * without this it is destroyed in reverse declaration order -- after
   * `root_dir`, which `get_root_fd()` dereferences -- and a cache
   * still holding a dirty entry segfaults the process on the way out.
   * It took the unit suite's second run to show it, because the cache
   * has to be non-empty at teardown for `drain()` to evict anything.
   *
   * Dropped here rather than reordered.  Reordering the members would
   * work and would leave nothing saying why the order matters, so the
   * next member added in the wrong place brings the crash back.
   *
   * Stabilizing on the way out is wanted, not merely tolerated:  under
   * writeback, eviction is the only thing that writes those part
   * records, so a shutdown that skipped it would lose the parts of
   * every multipart upload still in flight.  This makes it happen
   * while the driver can still serve it.
   *
   * posix has the same two members in the same order
   * (`rgw_sal_posix.h`) and an empty destructor, so it has the same
   * latent crash;  that line is rgw-standalone's and is noted rather
   * than changed here. */
  virtual ~NSFSDriver() {
    multipart_cache.reset();
    if (base_fd >= 0) {
      ::close(base_fd);
    }
  }

  const DoutPrefixProvider* get_dpp() const { return &driver_prefix; }

  /* What `who`'s copies are asked to do:  its bundle where it has
   * one, else the daemon's. */
  nsfs::CopyHints copy_hints_for(const rgw_owner& who);

  void set_context(CephContext *_cct) {
    cct = _cct;
  }

  NSFSDriver& set_run_lc_thread(bool _use_lc_thread) {
    use_lc_thread = _use_lc_thread;
    return *this;
  }

  /* RGW's `background_tasks`:  true in a process that serves requests,
   * radosgw and librgw;  false in radosgw-admin */
  NSFSDriver& set_background_tasks(bool _background_tasks) {
    background_tasks = _background_tasks;
    return *this;
  }

  bool have_fsio() const override { return true; }

  /* Construct the strategies, the profiles and the reserved-name union.
   * Called by initialize(), and directly by a test driver which does not
   * want the rest of initialization.  base_path_ must be set first.
   *
   * shares_extents replaces the filesystem probe when supplied, which
   * is how the strided staging layout is reached on a filesystem that
   * does share extents. */
  void init_strategies(const DoutPrefixProvider* dpp,
		       std::optional<bool> shares_extents = std::nullopt);

  /* Open the base path, creating it if it is missing, since buckets
   * default to it.  Called by initialize() and by a test driver, after
   * the strategies. */
  int open_base_path(const DoutPrefixProvider* dpp);

  /* Read rgw_posix_sync_policy and, under `relaxed`, start syncing every
   * filesystem that holds a bucket, beginning with the base path's.
   * Called by initialize() and by a test driver, after the base path is
   * open. */
  void init_sync_policy(const DoutPrefixProvider* dpp);

  /* A bucket's directory was opened:  under the relaxed policy its
   * filesystem joins the set that is synced.  `stx` is the directory's,
   * and `fd` a descriptor on it. */
  void note_bucket_filesystem(const DoutPrefixProvider* dpp,
			      const struct statx& stx, int fd);

  /* The filesystems the relaxed policy syncs;  empty under any other. */
  std::vector<dev_t> synced_filesystems() const {
    return sync_set ? sync_set->devices() : std::vector<dev_t>{};
  }

  /* Construct the multipart part cache.  Sizing is the caller's, because
   * it decides whether eviction -- and so the stabilize callback -- is
   * ever reached;  initialize() passes the configured values and a test
   * driver passes its own. */
  void init_multipart_cache(const DoutPrefixProvider* dpp,
			    uint64_t mp_max, uint64_t mp_lanes,
			    uint64_t mp_parts, uint64_t mp_max_parts,
			    file::listing::MultipartCachePolicy mp_policy);

  virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp);
  virtual const std::string get_name() const override { return "nsfs"; }
  virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y) override { return "PLACEHOLDER"; };
  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const
				     std::string& key, optional_yield y,
				     std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const
				std::string& email, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const
				std::string& user_str, optional_yield y,
				std::unique_ptr<User>* user) override;


  virtual int load_account_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view id,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv) override;
  virtual int load_account_by_name(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view name,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv) override;
  virtual int load_account_by_email(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view email,
				  RGWAccountInfo& info,
				  Attrs& attrs,
				  RGWObjVersionTracker& objv) override;

  virtual int store_account(const DoutPrefixProvider* dpp,
			  optional_yield y, bool exclusive,
			  const RGWAccountInfo& info,
			  const RGWAccountInfo* old_info,
			  const Attrs& attrs,
			  RGWObjVersionTracker& objv) override;

  virtual int delete_account(const DoutPrefixProvider* dpp,
			     optional_yield y,
			     const RGWAccountInfo& info,
			     RGWObjVersionTracker& objv) override;

  virtual int load_stats(const DoutPrefixProvider* dpp,
			 optional_yield y,
			 const rgw_owner& owner,
			 RGWStorageStats& stats,
			 ceph::real_time& last_synced,
			 ceph::real_time& last_updated) override { return 0; }
  virtual int load_stats_async(const DoutPrefixProvider* dpp,
			       const rgw_owner& owner,
			       boost::intrusive_ptr<ReadStatsCB> cb) override { return -ENOTSUP; }
  virtual int reset_stats(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const rgw_owner& owner) override { return -ENOTSUP; }
  virtual int complete_flush_stats(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   const rgw_owner& owner) override { return 0; }

  virtual int load_owner_by_email(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view email,
				  rgw_owner& owner) override;

  virtual int count_account_roles(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view account_id,
				  uint32_t& count) override;
  virtual int list_account_roles(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view account_id,
				 std::string_view path_prefix,
				 std::string_view marker,
				 uint32_t max_items,
				 RoleList& listing) override;

  virtual int load_account_user_by_name(const DoutPrefixProvider* dpp,
					optional_yield y,
					std::string_view account_id,
					std::string_view tenant,
					std::string_view username,
					std::unique_ptr<User>* user) override;
  virtual int count_account_users(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view account_id,
				  uint32_t& count) override;
  virtual int list_account_users(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view account_id,
				 std::string_view tenant,
				 std::string_view path_prefix,
				 std::string_view marker,
				 uint32_t max_items,
				 UserList& listing) override;

  virtual int load_group_by_id(const DoutPrefixProvider* dpp,
			       optional_yield y,
			       std::string_view id,
			       RGWGroupInfo& info, Attrs& attrs,
			       RGWObjVersionTracker& objv) override;
  virtual int load_group_by_name(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view account_id,
				 std::string_view name,
				 RGWGroupInfo& info, Attrs& attrs,
				 RGWObjVersionTracker& objv) override;
  virtual int store_group(const DoutPrefixProvider* dpp, optional_yield y,
			  const RGWGroupInfo& info, const Attrs& attrs,
			  RGWObjVersionTracker& objv, bool exclusive,
			  const RGWGroupInfo* old_info) override;
  virtual int remove_group(const DoutPrefixProvider* dpp, optional_yield y,
			   const RGWGroupInfo& info,
			   RGWObjVersionTracker& objv) override;
  virtual int list_group_users(const DoutPrefixProvider* dpp,
			       optional_yield y,
			       std::string_view tenant,
			       std::string_view id,
			       std::string_view marker,
			       uint32_t max_items,
			       UserList& listing) override;
  virtual int count_account_groups(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   uint32_t& count) override;
  virtual int list_account_groups(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view account_id,
				  std::string_view path_prefix,
				  std::string_view marker,
				  uint32_t max_items,
				  GroupList& listing) override;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  /* P2.  The Directory a placed bucket is served from, or null for
   * the default location.  Consulted on every lookup, because the
   * path is bound late. */
  std::unique_ptr<nsfs::Directory>
  placed_bucket_dir(const DoutPrefixProvider* dpp, const std::string& name,
		    const std::string& tenant);

  /* Every bucket's Directory:  placed by its row or NooBaa's record,
   * or else one with no path, which is -ENOENT. */
  std::unique_ptr<nsfs::Directory>
  bucket_dir(const DoutPrefixProvider* dpp, const std::string& name,
	     const std::string& tenant);

  /* The Directory for a bucket at an absolute path:  no parent, and
   * seeded with the strategies the base path's Directory is given. */
  std::unique_ptr<nsfs::Directory> make_placed_dir(const std::string& path);

  /* Where a new bucket goes, as NooBaa's CreateBucket decides it:  the
   * owner's new_buckets_path, or the base path for an owner created in
   * RGW (DECISIONS.md 2026-10-13).  A tenant's bucket is named
   * `<tenant>$<name>` there, since the directory name is the only thing
   * keeping two tenants apart.
   *
   * -EACCES for an owner imported from NooBaa without a path, which
   * NooBaa does not let create buckets.  -EEXIST for a name a NooBaa
   * bucket record holds, since that bucket is visible already. */
  int new_bucket_path(const DoutPrefixProvider* dpp, const rgw_owner& owner,
		      const rgw_bucket& b, std::string& path,
		      bool* owners_path = nullptr);

  /* Whose identity row supplies a new bucket's directory --
   * `new_buckets_path` and the custom bucket path allowlist -- for
   * new_bucket_path(), custom_bucket_path() and
   * check_owner_can_create_at():  the creating user's, as NooBaa's
   * CreateBucket reads the requesting record's, when
   * rgw_nsfs_user_identity_override is on and that user is in the
   * owning account and has a row (I9);  otherwise `owner`.  The bucket
   * is `owner`'s either way. */
  rgw_owner new_bucket_path_source(const DoutPrefixProvider* dpp,
				   const rgw_owner& owner,
				   const rgw_owner& authenticated_user,
				   optional_yield y);

  /* NooBaa's custom bucket path:  the header rgw_nsfs_custom_bucket_path_header
   * names, when the request carries it.  The path is admitted by the
   * owner's allowlist, or rgw_nsfs_custom_bucket_path_allowed_list when the
   * owner has none, as a string prefix (DECISIONS.md 2026-10-14).  0 with
   * `path` unset when there is no header;  -EACCES when no allowlist
   * admits it, as NooBaa denies it with UNAUTHORIZED;  -EINVAL for a path
   * nsfs would not store. */
  int custom_bucket_path(const DoutPrefixProvider* dpp, const rgw_owner& owner,
			 const RGWEnv* env, std::optional<std::string>& path);

  /* NooBaa's guard on where an account's buckets go, enacted:  NooBaa
   * makes a bucket's directory as the account, so an account that
   * cannot write the parent cannot create there.  nsfs makes it as the
   * gateway (NSFS_IMPERSONATION.md §8a), so the account's access is
   * checked as the account, the kernel deciding, ACLs included.  Under
   * impersonation only, and for a path the account chose -- its
   * new_buckets_path or a custom path -- never the base path, which
   * is the gateway's own default and was never the account's to
   * write.  -EACCES when the account may not write
   * `path`'s parent, the parent itself and not the path to it
   * (DECISIONS.md 2026-10-14, 2026-10-15). */
  int check_owner_can_create_at(const DoutPrefixProvider* dpp,
				const rgw_owner& owner, const std::string& path,
				optional_yield y);
  /* `mode` on the directory `path` itself as the owner, the directories
   * above it not consulted;  under impersonation only.  -EACCES when
   * denied, and when the owner's credentials do not resolve */
  int check_owner_access(const DoutPrefixProvider* dpp,
			 const rgw_owner& owner, const std::string& path,
			 int mode, optional_yield y);

  /* Place a directory nobody created through S3 or NooBaa as a bucket,
   * as NooBaa's `manage_nsfs bucket add --path` does (DECISIONS.md
   * 2026-10-14).  Writes the row and the bucket's state;  the
   * directory's contents are not touched, and deleting the bucket
   * leaves the directory.  -EINVAL for a path that is not an absolute
   * canonical directory, -ENOENT for one that does not exist,
   * -ERR_NO_SUCH_USER for an unknown owner, -EINVAL for an owner that is
   * a user rather than an account, -EACCES when the owner
   * cannot read and write it under impersonation, -EEXIST for a taken
   * name, a directory carrying nsfs state, or a path nested with another
   * bucket's -- a row's, or until adoption is complete a NooBaa
   * record's. */
  /* -EEXIST when `path` is the directory of a NooBaa bucket not yet
   * imported, or nested with it, until adoption is complete */
  int check_clear_of_noobaa_paths(const DoutPrefixProvider* dpp,
				  const std::string& name,
				  const std::string& path);

  int place_bucket(const DoutPrefixProvider* dpp, optional_yield y,
		   const std::string& name, const std::string& path,
		   const rgw_owner& owner, const RGWZoneGroup& zonegroup,
		   std::string* profile);

  virtual std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i)  override;
  /* What a client reaches:  a bucket whose adoption failed is refused,
   * and a request promotes a bucket not yet adopted. */
  virtual int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                          std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  /* The driver's own work -- adoption, abandonment, the profile
   * endpoints, the hints -- which must reach a bucket whose adoption
   * failed, and must not wait on adoption. */
  int load_bucket_internal(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                           std::unique_ptr<Bucket>* bucket, optional_yield y);
  /* Whether `bucket`'s adoption failed and no operator has acted since. */
  bool adoption_failed(const std::string& bucket);
  /* 0 when NooBaa's store as a whole admits adoption:  it reads, and
   * its release is one adoption is verified against (X4).  Otherwise
   * the error, with `why` set. */
  int store_admits_adoption(const DoutPrefixProvider* dpp, std::string& why);
  virtual int list_buckets(const DoutPrefixProvider* dpp,
			   const rgw_owner& owner, const std::string& tenant,
			   const std::string& marker, const std::string& end_marker,
			   uint64_t max, bool need_stats, BucketList& buckets,
			   optional_yield y) override;
  virtual bool is_meta_master() override { return true; }
  virtual Zone* get_zone() override { return &zone; }
  virtual std::string zone_unique_id(uint64_t unique_num) override { return ""; }
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
  virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
  virtual int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) override;
  virtual int cluster_stat(RGWClusterStat& stats) override;
  virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
  virtual std::unique_ptr<Restore> get_restore(void) { return nullptr; }
  virtual bool process_expired_objects(const DoutPrefixProvider *dpp, optional_yield y) override { return 0; }

  virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
      rgw::notify::EventType event_type, optional_yield y, const std::string* object_name=nullptr) override;
  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider* dpp,
      rgw::sal::Object* obj,
      rgw::sal::Object* src_obj,
      const rgw::notify::EventTypeList& event_types,
      rgw::sal::Bucket* _bucket,
      std::string& _user_id,
      std::string& _user_tenant,
      std::string& _req_id,
      optional_yield y) override;
  int read_topic_v2(const std::string& topic_name,
		    const std::string& tenant,
		    rgw_pubsub_topic& topic,
		    RGWObjVersionTracker* objv_tracker,
		    optional_yield y,
		    const DoutPrefixProvider* dpp) override;
  int write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
		     RGWObjVersionTracker& objv_tracker,
		     optional_yield y,
		     const DoutPrefixProvider* dpp) override;
  int remove_topic_v2(const std::string& topic_name,
		      const std::string& tenant,
		      RGWObjVersionTracker& objv_tracker,
		      optional_yield y,
		      const DoutPrefixProvider* dpp) override;
  int update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
				  const std::string& bucket_key,
				  bool add_mapping,
				  optional_yield y,
				  const DoutPrefixProvider* dpp) override;
  int get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
			       std::set<std::string>& bucket_keys,
			       optional_yield y,
			       const DoutPrefixProvider* dpp) override;
  int remove_bucket_mapping_from_topics(
      const rgw_pubsub_bucket_topics& bucket_topics,
      const std::string& bucket_key,
      optional_yield y,
      const DoutPrefixProvider* dpp) override;
  virtual int list_account_topics(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view account_id,
				  std::string_view marker,
				  uint32_t max_items,
				  TopicList& listing) override;

  virtual int add_persistent_topic(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   const std::string& topic_queue) override { return -ENOTSUP; }
  virtual int remove_persistent_topic(const DoutPrefixProvider* dpp,
				      optional_yield y,
				      const std::string& topic_queue) override { return -ENOTSUP; }

  virtual RGWLC* get_rgwlc(void) override { return lc; }
  virtual rgw::restore::Restore* get_rgwrestore(void) { return nullptr; }
  virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }

  virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y) override { return 0; }
  virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override { return 0; }
  virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
				      const std::map<std::string, std::string>& meta) override { return 0; }
  virtual void get_quota(RGWQuota& quota) override { return ; }
  virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) override { return; }
  virtual int set_buckets_enabled(const DoutPrefixProvider* dpp, std::vector<rgw_bucket>& buckets, bool enabled, optional_yield y) override { return 0; } // TODO: implement
  virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
				      std::optional<rgw_zone_id> zone,
				      std::optional<rgw_bucket> bucket,
				      RGWBucketSyncPolicyHandlerRef* phandler,
				      optional_yield y) override { return 0; }
  virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override { return 0; }
  virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override { return; }
  virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override { return; }
  virtual int clear_usage(const DoutPrefixProvider *dpp, optional_yield y) override { return 0; }
  virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			     uint32_t max_entries, bool* is_truncated,
			     RGWUsageIter& usage_iter,
			     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override { return 0; }
  virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override { return 0; }
  virtual int get_config_key_val(std::string name, bufferlist* bl) override { return -ENOTSUP; }
  virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) override;
  virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) override;
  virtual void meta_list_keys_complete(void* handle) override;
  virtual std::string meta_get_marker(void* handle) override { return ""; }
  virtual int meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y) override { return 0; }
  virtual const RGWSyncModuleInstanceRef& get_sync_module() override { return sync_module; }
  virtual std::unique_ptr<LuaManager> get_lua_manager(const std::string& luarocks_path) override;
  virtual std::unique_ptr<RGWRole> get_role(std::string name,
					    std::string tenant,
					    rgw_account_id account_id,
					    std::string path="",
					    std::string trust_policy="",
					    std::string description="",
					    std::string max_session_duration_str="",
	      std::multimap<std::string,std::string> tags={}) override;
  virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
  virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
  virtual int list_roles(const DoutPrefixProvider *dpp,
			 optional_yield y,
			 const std::string& tenant,
			 const std::string& path_prefix,
			 const std::string& marker,
			 uint32_t max_items,
			 RoleList& listing) override;
  virtual int store_oidc_provider(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  const RGWOIDCProviderInfo& info,
                                  bool exclusive,
                                  RGWObjVersionTracker* objv_tracker) override;
  virtual int load_oidc_provider(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view tenant,
                                 std::string_view url,
                                 RGWOIDCProviderInfo& info,
                                 RGWObjVersionTracker* objv_tracker) override;
  virtual int delete_oidc_provider(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view tenant,
				   std::string_view url) override;
  virtual int get_oidc_providers(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::vector<RGWOIDCProviderInfo>& providers) override;
  virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				optional_yield y,
				rgw::sal::Object* obj,
				const ACLOwner& owner,
				const rgw_placement_rule *ptail_placement_rule,
				const std::string& unique_tag,
				uint64_t position,
				uint64_t *cur_accounted_size,
				const rgw_owner& authenticated_user) override;
  virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				optional_yield y,
				rgw::sal::Object* obj,
				const ACLOwner& owner,
				const rgw_placement_rule *ptail_placement_rule,
				uint64_t olh_epoch,
				const std::string& unique_tag,
				const rgw_owner& authenticated_user) override;

  virtual const std::string& get_compression_type(const rgw_placement_rule& rule) override;
  virtual bool valid_placement(const rgw_placement_rule& rule) override { return true; }

  virtual void shutdown(void) override;
  virtual void finalize(void) override;

  virtual CephContext* ctx(void) override { return userDB->ctx(); }

  virtual void register_admin_apis(RGWRESTMgr* mgr) override;
  void get_features(std::map<std::string, std::string>& features) override;

  /* Take a slot for a mutation on this bucket.
   *
   * Waits while the gate is closed and gives up rather than waiting
   * out a whole conversion, which is the shape resharding already
   * uses:  guard_reshard() blocks for a bound and then lets a
   * retryable error reach the client.  -ERR_SERVICE_UNAVAILABLE is
   * 503 ServiceUnavailable, which every SDK retries.
   *
   * Paired with mutation_leave(), or held through NSFSMutationGuard. */
  int mutation_enter(const DoutPrefixProvider* dpp, NSFSBucket* bucket,
		     MutationPurpose purpose);
  /* The gate's question alone, without a slot:  0, or
   * -ERR_INVALID_BUCKET_STATE for a client's write to a bucket not taken
   * over.  mutation_enter() asks it, and so does a path that writes
   * before it takes its slot -- UploadPart writes the part's bytes before
   * its writer's complete(). */
  int reject_unadopted(const DoutPrefixProvider* dpp, NSFSBucket* bucket,
		       MutationPurpose purpose);
  /* Wait, as a write waits at a closed gate, until `done()` holds or the
   * bound passes;  woken when a conversion opens the gate again.  A write
   * that needs a conversion first waits for it and converts nothing
   * (DECISIONS.md 2026-10-21).  Never called holding a slot:  the
   * conversion drains slots before it walks.  0, or
   * -ERR_SERVICE_UNAVAILABLE. */
  int wait_for_conversion(const DoutPrefixProvider* dpp,
			  const std::string& bucket,
			  const std::function<bool()>& done);
  void mutation_leave(const std::string& bucket);

  /* Hold mutations off, and wait for the ones already running.
   *
   * Returns once the bucket has none in flight, which is what makes
   * it safe to rewrite the tree.  -ETIMEDOUT if something is still
   * running after the bound, and the gate is left open again -- a
   * conversion that cannot start is better than one that starts
   * beside a writer. */
  int gate_close(const DoutPrefixProvider* dpp, const std::string& bucket);
  void gate_open(const std::string& bucket);
  bool gate_is_closed(const std::string& bucket);

  /* Test support.  A test which means to observe a rejected mutation
   * would otherwise wait out the production timeout, and shortening
   * the constant would shorten it for a deployment too. */
  void set_gate_waits(std::chrono::milliseconds mutation,
		      std::chrono::milliseconds drain) {
    mutation_gate_wait = mutation;
    gate_drain_wait = drain;
  }

  AdoptionMode get_adoption_mode() const { return adoption_mode; }

  /* Whether the mode governs anything.  It does not without a store
   * to adopt from, which is what keeps this option from changing an
   * ordinary deployment by existing. */
  /* Adoption's completion record, read once at startup and updated by
   * the pass that writes it.  Once set, NooBaa's store is not read:  no
   * record serves a bucket, lists one, holds a name, or is adopted. */
  bool adoption_complete() const {
    return completed.load(std::memory_order_acquire);
  }
  std::optional<nsfs::AdoptionCompletion> adoption_completion() const {
    std::lock_guard l{completion_mtx};
    return completion;
  }
  /* Read the record from the identity DB.  Called by initialize() and a
   * test driver, and by each adoption command before it acts, which is
   * how a record another gateway wrote is seen. */
  int load_adoption_completion(const DoutPrefixProvider* dpp);
  /* adoption_complete(), re-reading the record first when this gateway
   * has not seen it:  a read of NooBaa's store is the reason to look,
   * since another gateway may have written it. */
  bool reread_adoption_complete(const DoutPrefixProvider* dpp);
  /* An operator's decision that one NooBaa record goes no further,
   * with the reason.  -EINVAL with no reason;  -ENOENT for an `_id` the
   * store does not hold, or no store;  -EEXIST for a record fully
   * consumed, one already abandoned, or once adoption is complete. */
  int abandon_noobaa_record(const DoutPrefixProvider* dpp, optional_yield y,
			    const std::string& noobaa_id,
			    const std::string& reason,
			    nsfs::AbandonedRecord& out);
  /* -ENOENT when it is not abandoned;  -EEXIST once adoption is
   * complete. */
  int unabandon_noobaa_record(const DoutPrefixProvider* dpp,
			      const std::string& noobaa_id);
  int list_abandoned(const DoutPrefixProvider* dpp,
		     std::vector<nsfs::AbandonedRecord>& out);
  /* The abandoned entry for NooBaa's record of `bucket_name`.  -ENOENT
   * when there is no record, or it is not abandoned. */
  int abandoned_bucket(const DoutPrefixProvider* dpp,
		       const std::string& bucket_name,
		       nsfs::AbandonedRecord& out);

  int observe_adoption_completion(const DoutPrefixProvider* dpp,
				  optional_yield y, const std::string& root,
				  const nsfs::NooBaaStoreContents& contents,
				  std::map<std::string, std::string>* report);

  bool adoption_configured() const {
    return nb_bucket_state_strategy != nullptr;
  }

  /* Adopt the whole store:  its identities, then every bucket.  What
   * `automatic` runs at startup in a process with no background tasks;
   * one that serves adopts through the queue instead. */
  int run_adoption(const DoutPrefixProvider* dpp, optional_yield y,
		   std::map<std::string, std::string>* report = nullptr);

  /* Import the store's identities, and nothing else.  Store-wide and
   * first, because every bucket's owner and every policy principal
   * resolves through them. */
  int adopt_identities(const DoutPrefixProvider* dpp, optional_yield y,
		       std::map<std::string, std::string>* report = nullptr);

  /* Adopt one bucket now, against identities already imported:  its
   * profile out of base, which imports its NooBaa record and rewrites
   * its tree.  With extensions off a tree stays base, so only the
   * record is imported. */
  int adopt_one_bucket(const DoutPrefixProvider* dpp, optional_yield y,
		       const std::string& bucket_name,
		       std::map<std::string, std::string>* report = nullptr,
		       nsfs::convert_cb_t* cb = nullptr);

  /* `automatic` while serving:  the identities now, every bucket
   * through the queue.  -EAGAIN when the identities do not import, and
   * nothing is queued;  0 with nothing done when a queue is running.
   * `workers` overrides rgw_nsfs_adoption_workers. */
  int start_adoption_queue(const DoutPrefixProvider* dpp,
			   std::optional<uint64_t> workers = std::nullopt);
  /* Whether a request on `b` should promote its adoption:  the queue
   * runs, and `b` is NooBaa's, not yet imported and not abandoned. */
  bool awaits_adoption(const DoutPrefixProvider* dpp, NSFSBucket& b);

  /* Import one bucket's NooBaa record:  the half of a bucket's adoption
   * that set_profile() runs before it rewrites the tree.  -ENOENT when
   * the store holds no record for it. */
  int import_noobaa_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			   const std::string& bucket_name,
			   std::map<std::string, std::string>* report = nullptr);

  /* Leave out the identities an operator abandoned.  Returns how many. */
  size_t drop_abandoned_identities(const DoutPrefixProvider* dpp,
				   nsfs::NooBaaStoreContents& contents);
  /* NooBaa's principals as RGW names them, for the identities the import
   * has written:  what adoption rewrites a bucket policy through. */
  int imported_identity_mapping(const DoutPrefixProvider* dpp,
				const nsfs::NooBaaStoreContents& contents,
				nsfs::IdentityMapping& out);

  /* A bucket served in NooBaa's format serves its policy with the same
   * principals adoption will write:  NooBaa's names name nobody in RGW.
   * A policy naming anyone the import did not write is not served, as
   * the import does not write it (`ACCOUNT_IMPORT.md` §6.1a).  0, or
   * the error that kept the mapping from being built. */
  int rewrite_unadopted_policy(const DoutPrefixProvider* dpp,
			       const std::string& bucket, Attrs& attrs);

  /* The mapping rewrite_unadopted_policy() uses is built from the whole
   * store, once, and rebuilt after an identities import, the only thing
   * that changes it. */
  void invalidate_unadopted_mapping();

  int driver_hint(const DoutPrefixProvider* dpp,
		  const std::string& hint,
		  const std::map<std::string, std::string>& params,
		  std::map<std::string, std::string>* out = nullptr) override;

  bool fork_race_injected() const { return inject_fork_race; }
  int64_t stored_size_delta_injected(const std::string& upload) {
    std::lock_guard l{inject_delta_lock};
    auto it = inject_delta_by_upload.find(upload);
    return it == inject_delta_by_upload.end() ? 0 : it->second;
  }
  /* how many parts have been moved out of a shared staging file
   * because they would have crossed their extent.  Reported by the
   * mpu-state hint:  a divert is correct behaviour and leaves no trace
   * a test can see otherwise, since the object it produces is right. */
  void record_mpu_divert() { ++mpu_diverts; }
  uint64_t mpu_diverts_seen() const { return mpu_diverts.load(); }
  void set_stored_size_delta(const std::string& upload, int64_t bytes) {
    std::lock_guard l{inject_delta_lock};
    if (bytes == 0) {
      inject_delta_by_upload.erase(upload);
    } else {
      inject_delta_by_upload[upload] = bytes;
    }
  }
  bool skip_reclone_injected() const { return inject_skip_reclone; }
  bool etag_as_gateway_injected() const { return inject_etag_as_gateway; }
  bool denial_as_absence_injected() const { return inject_denial_as_absence; }
  bool unresolved_as_gateway_injected(const rgw_owner& who) {
    std::lock_guard l{unresolved_as_gateway_lock};
    return !unresolved_as_gateway_user.empty() &&
	   (unresolved_as_gateway_user == to_string(who));
  }
  bool no_chown_injected() const { return inject_no_chown; }
  bool create_failure_injected() const { return inject_create_failure; }
  bool attr_read_fail_injected(const std::string& bucket) {
    if (!inject_attr_read_fail_armed.load(std::memory_order_relaxed)) {
      return false;
    }
    std::lock_guard l{inject_attr_read_fail_lock};
    return inject_attr_read_fail_buckets.contains(bucket);
  }
  void record_attr_read_fail() { ++attr_read_fails_injected; }
  bool attr_write_fail_injected(const std::string& bucket) {
    if (!inject_attr_write_fail_armed.load(std::memory_order_relaxed)) {
      return false;
    }
    std::lock_guard l{inject_attr_write_fail_lock};
    return inject_attr_write_fail_buckets.contains(bucket);
  }
  void record_attr_write_fail() { ++attr_write_fails_injected; }
  bool adoption_gate_lifted(const std::string& bucket) {
    if (!inject_lift_adoption_gate_armed.load(std::memory_order_relaxed)) {
      return false;
    }
    std::lock_guard l{inject_lift_adoption_gate_lock};
    return inject_lift_adoption_gate_buckets.contains(bucket);
  }
  int rename_fail_after_injected() const { return inject_rename_fail_after; }
  int rename_abandon_injected() const { return inject_rename_abandon; }
  bool fsio_attrs_fail_injected() const { return inject_fsio_attrs_fail; }
  bool fsio_link_fail_injected() const { return inject_fsio_link_fail; }
  bool fsio_skip_cleanup_injected() const { return inject_fsio_skip_cleanup; }

  /* Record what this user's operations choose, discarding
   * anything held for a previous one.  See ran_as_lock. */
  void select_ran_as(const std::string& who) {
    std::lock_guard l{ran_as_lock};
    ran_as_who = who;
    ran_as_seen.clear();
  }
  /* Record the identity `site` chose, if this is the selected
   * user.
   * An inactive identity is the gateway, which is the answer when
   * impersonation is off and is worth recording rather than
   * dropping:  the test then has both polarities. */
  void note_ran_as(const char* site, const std::string& who,
		   const nsfs::FSIdentity& id) {
    std::lock_guard l{ran_as_lock};
    if (ran_as_who.empty() || (who != ran_as_who)) {
      return;
    }
    ran_as_seen.emplace_back(site,
			     id.active() ? id.cred.uid : ::geteuid());
  }
  /* Drain, and stop selecting:  a read answers for the operations
   * between the select and itself and for nothing after them. */
  std::vector<std::pair<std::string, uid_t>> take_ran_as() {
    std::lock_guard l{ran_as_lock};
    ran_as_who.clear();
    return std::move(ran_as_seen);
  }

  /* Internal APIs */
  rgw::store::POSIXUserDB* get_user_db() { return userDB.get(); }
  nsfs::IdentityDB* get_identity_db() { return userDB.get(); }
  const nsfs::AccessStrategy* get_access_strategy() const {
    return access_strategy_live.load(std::memory_order_relaxed);
  }

  /* How many operations the access model let the gateway perform
   * after the filesystem denied the authenticated user, by class.
   *
   * Reported by the `access-model` hint.  Without it a test that
   * sees a read succeed cannot tell *why*:  because the model
   * permitted it, or because the filesystem never objected and the
   * fixture proves nothing.  The counts make a success
   * attributable. */
  std::atomic<uint64_t> cross_account_attrs_served{0};
  std::atomic<uint64_t> cross_account_data_served{0};

  /* How many publishes the gateway performed itself, which the
   * access model permits.
   *
   * Separate from the two counts above, which record a denial the
   * model turned into service.  The publish never learns of a denial
   * -- it is decided from the model before the first syscall, because
   * its first step spends the O_TMPFILE's one link
   * (NSFSObject::link_temp_file) -- so this counts what the gateway
   * did and not what the filesystem said. */
  std::atomic<uint64_t> publishes_as_gateway{0};

  /* The POSIX credentials `who` is served as, by the same resolution
   * every operation for it uses:  a user's own row under the user
   * identity override, else its account's;  a role session's
   * (rgw::sal::role_session_ns) own row under the role identity
   * override, else its account's;  an account's row.  For a request,
   * `who` is authenticated_user(s).  Returns what
   * nsfs::resolve_credentials() returns:  0 resolved, -ENOENT no
   * impersonation asked for, -EPERM asked for and unavailable.  `row`
   * receives the key of the row that resolved. */
  int get_credentials_for(const DoutPrefixProvider* dpp, const rgw_owner& who,
			  optional_yield y, nsfs::Credentials& out,
			  std::string* row = nullptr);
  /* Where a bucket goes whose owner has no new_buckets_path, read once
   * at creation and copied into its row. */
  const std::string& default_bucket_location() const { return base_path_; }
  /* A path on the filesystem the buckets are on, for what measures or
   * acts on a filesystem rather than a bucket:  the GPFS check, the
   * extents probe, statvfs, the bucket cache's watch, and the relaxed
   * sync set's first entry.  One filesystem type per deployment is
   * assumed;  one filesystem is not, which is why the sync set learns
   * the others as buckets are opened. */
  const std::string& fs_reference_path() const { return base_path_; }
  nsfs::BucketCache* get_bucket_cache() { return bucket_cache.get(); }
  nsfs::MultipartCache* get_multipart_cache() { return multipart_cache.get(); }
  UserCache& get_user_cache() { return user_cache; }
  nsfs::FSStrategy* get_fs_strategy() { return fs_strategy.get(); }
  nsfs::MPUStrategy* get_mpu_strategy() { return mpu_strategy.get(); }
  nsfs::XattrStrategy* get_xattr_strategy() { return xattr_strategy.get(); }
  nsfs::PathStrategy* get_path_strategy() { return path_strategy.get(); }
  /* The foreign readers.  Reached only through a bucket that is
   * mid-upgrade;  see NSFSBucket::xattr_fallback(). */
  nsfs::BucketStateStrategy* get_bucket_state_strategy() {
    return bucket_state_strategy.get();
  }
  nsfs::BucketStateStrategy* get_noobaa_bucket_state_strategy() {
    return nb_bucket_state_strategy.get();
  }
  nsfs::XattrStrategy* get_noobaa_xattr_strategy() {
    return nb_xattr_strategy.get();
  }
  nsfs::PathStrategy* get_noobaa_path_strategy() {
    return nb_path_strategy.get();
  }
  nsfs::MPUStrategy* get_noobaa_mpu_strategy() {
    return nb_mpu_strategy.get();
  }
  const nsfs::ReservedNames& get_reserved_names() const { return reserved_names; }

  /* The profile an extension set selects, or nullptr for a bucket this
   * build cannot serve -- rejected rather than guessed.  Base is not
   * reachable from here:  it is the absence of the attribute, and
   * load_bucket resolves it without asking.
   *
   * The profile objects differ only in which strategies they point
   * at, and today all of them point at the same ones.  When S5 makes them
   * differ, a bucket carrying a *subset* of the known bits will need to
   * carry its own mask rather than borrow the profile's;  the profile
   * selects implementations, the mask says what the bucket has. */
  const nsfs::BucketProfile* resolve_profile(uint32_t extensions) const {
    if (extensions & ~nsfs::EXTENSIONS_KNOWN) {
      return nullptr;
    }
    /* An attribute holding zero names no profile.  Base is the absence
     * of the attribute, and nothing we write produces an empty mask, so
     * a zero here is a tree somebody edited or a truncated write. */
    if (extensions == nsfs::EXTENSIONS_NONE) {
      return nullptr;
    }
    /* Anything adding structure is strong.  A mask holding only
     * EXT_RGW_META changes attribute names and packing and adds no file,
     * which is what shared is. */
    if (extensions & ~nsfs::EXT_RGW_META) {
      return &extended_profile;
    }
    return &shared_profile;
  }
  const nsfs::BucketProfile* get_base_profile() const { return &base_profile; }

  /* What a bucket created here is marked with:  rgw_nsfs_bucket_profile.
   * Read per call, so a test can select strong for a bucket it is about
   * to create. */
  uint32_t new_bucket_extensions() {
    auto ext = nsfs::extensions_for_profile(
      ctx()->_conf.get_val<std::string>("rgw_nsfs_bucket_profile"));
    return (ext && (*ext != nsfs::EXTENSIONS_BASE)) ? *ext
						      : nsfs::EXTENSIONS_SHARED;
  }

  /* Write the marker onto an existing bucket directory.  Idempotent for a
   * bucket already at this version. */
  /* Set a named bucket's profile, and report what it was so a caller
   * can tell a change from a no-op. */
  int set_bucket_profile(const DoutPrefixProvider* dpp, optional_yield y,
			 const std::string& name, uint32_t target,
			 uint32_t* had /* OUT */,
			 std::string* had_profile /* OUT */,
			 nsfs::ConvertProgress* progress = nullptr /* OUT */);

  /* Read it without changing it. */
  int get_bucket_profile(const DoutPrefixProvider* dpp, optional_yield y,
			 const std::string& name,
			 uint32_t* extensions /* OUT */,
			 std::string* pname /* OUT */,
			 bool* converting = nullptr /* OUT */);

  /* called by nsfs::BucketCache layer when a new object is discovered
   * by inotify or similar */
  int mint_listing_entry(
    const std::string& bucket, rgw_bucket_dir_entry& bde /* OUT */);

  RGWQuotaHandler* get_quota_handler() {return quota_handler;}
};

/* The one key to NSFSBucket::unmark_extensions().
 *
 * An unmarked bucket made by RGW is the state the gate exists to keep
 * clients out of (DECISIONS.md 2026-10-21), and tests need one.  So the
 * capability is contained by the compiler rather than by review:  the
 * constructor is private and its only friend is the hint that makes a
 * test's unmarked bucket, which takes only an empty bucket this gateway
 * marked and NooBaa holds no record of.  Any other call does not
 * compile. */
class UnmarkPasskey {
  friend int NSFSDriver::hint_unmarked_bucket(
      const DoutPrefixProvider* dpp,
      const std::map<std::string, std::string>& params,
      std::map<std::string, std::string>* out);
  UnmarkPasskey() = default;
};

class NSFSUser : public StoreUser {
private:
  NSFSDriver* driver;
  int load_user_from_cache_or_db(const DoutPrefixProvider* dpp, bool& cache_hit);
  /* I9:  a new account user's copy of its account's identity row */
  int copy_account_identity(const DoutPrefixProvider* dpp);

public:
  NSFSUser(NSFSDriver* _driver) :
    StoreUser(), driver(_driver) {}
  NSFSUser(NSFSDriver* _driver, const rgw_user& _u) :
    StoreUser(_u), driver(_driver) {}
  NSFSUser(NSFSDriver* _driver, const RGWUserInfo& _info) :
    StoreUser(_info), driver(_driver) {}
  virtual ~NSFSUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new NSFSUser(*this));
  }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override;
  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
                         uint64_t end_epoch, uint32_t max_entries,
                         bool* is_truncated, RGWUsageIter& usage_iter,
                         std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override { return 0; }
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
                         uint64_t end_epoch, optional_yield y) override { return 0; }
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int verify_mfa(const std::string& mfa_str, bool* verified,
                         const DoutPrefixProvider* dpp, optional_yield y) override;
  int list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                  std::string_view marker, uint32_t max_items,
                  GroupList& listing) override;
};

/* A mutation's slot, held for the length of the operation.
 *
 * Constructed at the top of a mutating SAL method;  the caller checks
 * result() and returns it if it is negative.  Nothing is held when it
 * is, so the destructor has nothing to release. */
class NSFSMutationGuard {
  NSFSDriver* driver;
  std::string bucket;
  int rc;

public:
  NSFSMutationGuard(const DoutPrefixProvider* dpp, NSFSDriver* _driver,
		    NSFSBucket* _bucket,
		    MutationPurpose purpose = MutationPurpose::client);
  ~NSFSMutationGuard() {
    if ((rc == 0) && driver) {
      driver->mutation_leave(bucket);
    }
  }
  NSFSMutationGuard(const NSFSMutationGuard&) = delete;
  NSFSMutationGuard& operator=(const NSFSMutationGuard&) = delete;

  /* Movable, because a slot outlives the scope that took it when the
   * mutation is an open write handle rather than a request:  see
   * NSFSObject::get_fsio_handle().  The moved-from guard releases
   * nothing. */
  NSFSMutationGuard(NSFSMutationGuard&& o) noexcept
    : driver(o.driver), bucket(std::move(o.bucket)), rc(o.rc) {
    o.driver = nullptr;
  }

  int result() const { return rc; }
};

class NSFSBucket : public StoreBucket {
private:
  NSFSDriver* driver;
  RGWAccessControlPolicy acls;
  std::optional<std::string> ns{std::nullopt};
  std::unique_ptr<nsfs::Directory> dir;
  /* resolved from the extensions marker;  null until it has been read */
  const nsfs::BucketProfile* profile{nullptr};
  /* read beside it:  the format this tree is being converted from, or
   * none.  Only meaningful once profile is non-null. */
  bool converting{false};
  /* default_object_owner(), once looked up */
  std::optional<ACLOwner> default_owner;

public:
  /* Whether this is a bucket of RGW's own, such as an upload's staging
   * directory, rather than a client's:  its keys are not clients'
   * names, so nsfs::resolve_key_path() does not reject reserved ones. */
  bool is_internal() const { return ns.has_value(); }

  NSFSBucket(NSFSDriver *_dr, nsfs::Directory* _p_dir, const rgw_bucket& _b, std::optional<std::string> _ns = std::nullopt)
    : StoreBucket(_b),
    driver(_dr),
    acls(),
    ns(_ns),
    dir(std::make_unique<nsfs::Directory>(get_fname(), _p_dir, _dr->ctx()))
    { }

  NSFSBucket(NSFSDriver *_dr, std::unique_ptr<nsfs::Directory> _this_dir, const rgw_bucket& _b, std::optional<std::string> _ns = std::nullopt)
    : StoreBucket(_b),
    driver(_dr),
    acls(),
    ns(_ns),
    dir(std::move(_this_dir))
    { }

  NSFSBucket(NSFSDriver *_dr, nsfs::Directory* _p_dir, const RGWBucketInfo& _i)
    : StoreBucket(_i),
    driver(_dr),
    acls(),
    ns(),
    dir(std::make_unique<nsfs::Directory>(get_fname(), _p_dir, _dr->ctx()))
    { }

  /* A bucket served from a Directory already built, with the info
   * provided -- P2's placed branch, where the directory is an absolute
   * path rather than a child of the base path, so it cannot be
   * rendered from the name. */
  NSFSBucket(NSFSDriver *_dr, std::unique_ptr<nsfs::Directory> _this_dir,
	     const RGWBucketInfo& _i)
    : StoreBucket(_i),
    driver(_dr),
    acls(),
    ns(),
    dir(std::move(_this_dir))
    { }

  NSFSBucket(const NSFSBucket& _b) :
    StoreBucket(_b),
    driver(_b.driver),
    acls(_b.acls),
    ns(_b.ns),
    profile(_b.profile)
    {
      dir = _b.dir->clone_dir();
    }

  virtual ~NSFSBucket() { }

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  virtual int list(const DoutPrefixProvider* dpp, ListParams&, int,
		   ListResults&, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp,
				    Attrs& new_attrs, optional_yield y) override;
  /* The same, for a writer that is not a client:  adoption writes a
   * bucket's state before it marks it (MutationPurpose). */
  int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs,
			    optional_yield y, MutationPurpose purpose);
  virtual int remove(const DoutPrefixProvider* dpp, bool delete_children,
		     optional_yield y,
		     const rgw_owner& authenticated_user) override;
  virtual int remove_bypass_gc(int concurrent_max,
			       bool keep_index_consistent,
			       optional_yield y,
			       const DoutPrefixProvider *dpp) override;
  virtual int create(const DoutPrefixProvider* dpp,
		     const CreateParams& params,
		     optional_yield y,
		     const rgw_owner& authenticated_user) override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y) override;

  /* Read the extensions marker and resolve it, if that has not happened
   * already.  Returns -ENOTSUP for a marker this build does not
   * implement:  a bucket written by a newer gateway is rejected, not
   * served on the assumption that its additions are ignorable. */
  int resolve_profile(const DoutPrefixProvider* dpp);
  const nsfs::BucketProfile* get_profile() const { return profile; }

  /* Whether this bucket carries the nsfs extensions at all -- shared or
   * strong, not base.  Unresolved reads as base, which is the safe
   * direction:  the interlocks reject. */
  bool extended() const { return profile && profile->extended(); }

  /* Whether it carries one particular extension.  What an interlock
   * guarding a single structure should ask:  extended() is true for
   * shared, which has the nsfs attribute names and none of the structures. */
  bool has_extension(uint32_t ext) const {
    return profile && profile->has(ext);
  }

  /* FSIO needs the shadow subtree, so only strong serves it.  librgw
   * asks this before an FSIO open;  fsio_allowed() is the same
   * rule inside the driver. */
  bool have_fsio() const override { return has_extension(nsfs::EXT_SHADOW); }

  const char* profile_name() const {
    return profile ? profile->name() : "unresolved";
  }

  const nsfs::Format* get_format() const {
    return profile ? profile->format : nullptr;
  }

  /* Test support:  point a bucket at a profile the driver does not
   * hold, so the selection can be shown choosing between two formats
   * while only one real one exists.  The caller owns the profile. */
  void set_profile_for_test(const nsfs::BucketProfile* p) {
    profile = p;
    apply_format();
  }

  /* Point the bucket's directory at the format's strategies.
   *
   * An FSEnt carries its own and every child inherits its parent's, so
   * seeding the bucket's directory is what puts a whole subtree in the
   * bucket's format.  Called wherever `profile` changes.  Without it
   * the subtree keeps the driver's, which are seeded on the root at
   * initialize() and are nsfs's -- the wrong format for every object
   * read and written through an FSEnt in a base bucket. */
  void apply_format();

  /* The strategies this bucket's format selects.
   *
   * They fall back to the driver's, for two reasons:  a bucket whose
   * profile has not been resolved yet has no format, and only one
   * format exists, so the results coincide.  Call sites move onto
   * these as each NooBaa strategy arrives and gives the selection
   * something to be wrong about;  migrating them while every path
   * resolves to the same objects would be a large diff nothing can
   * check.
   *
   * The MPU one differs:  a format which does not name a staging
   * layout takes the driver's, because per-part against strided is
   * chosen from what the filesystem can do. */
  nsfs::XattrStrategy* xattr_strategy() const;
  nsfs::PathStrategy* path_strategy() const;
  nsfs::MPUStrategy* mpu_strategy() const;
  nsfs::BucketStateStrategy* bucket_state_strategy() const;
  /* The fallback bucket-state strategy, or null.
   *
   * Runs the opposite way round to the object strategies, and
   * deliberately.  A
   * base bucket's *objects* are in NooBaa's format, so NooBaa's is the
   * primary there.  A base bucket's *state* is nsfs's where it exists,
   * because bucket configuration is the one thing written in the nsfs
   * form while a bucket is still base -- so the nsfs strategy is the
   * primary here and NooBaa's store is read only for a bucket that has
   * none. */
  nsfs::BucketStateStrategy* bucket_state_fallback() const;

  /* The fallback strategies, or null.
   *
   * A bucket being converted holds objects in two formats and its mark
   * cannot say which an individual object is in, so a reader asks the
   * bucket's own strategy first and these second.  Null on every
   * bucket that is not mid-conversion, which is every bucket we
   * created and every one whose conversion has finished -- so the
   * fallback costs nothing to have and is unreachable rather than
   * merely unused.
   *
   * Not a vector.  Two formats exist, so there is one fallback and
   * never a sequence of them, and a container would put an
   * indirection on a path that has one element in every case but
   * this one. */
  nsfs::XattrStrategy* xattr_fallback() const;
  nsfs::PathStrategy* path_fallback() const;
  nsfs::MPUStrategy* mpu_fallback() const;
  /* Whether the upload `meta` is still where NooBaa staged it in a bucket
   * whose adoption is underway:  the conversion has not reached it, and
   * a write to it waits.  Conversion removes NooBaa's staging for an
   * upload once it has re-staged it. */
  bool upload_awaits_conversion(const DoutPrefixProvider* dpp,
				const std::string& meta);
  bool is_converting() const { return converting; }

  /* Move the bucket to a profile.  The one way a bucket's profile
   * changes:  adding extensions is a write, dropping them requires the
   * bucket to hold no content and removes the scaffolding the dropped
   * extensions own.
   *
   * Leaving base is the upgrade, and it rewrites the tree.  `cb`, when
   * given, is called for each item and phase;  see ConvertEvent.  The
   * REST path passes none.  `progress`, when given, is filled in
   * whether the run finished or stopped. */
  int set_profile(const DoutPrefixProvider* dpp, optional_yield y,
		  uint32_t target,
		  nsfs::convert_cb_t* cb = nullptr,
		  nsfs::ConvertProgress* progress = nullptr);

  /* Rewrite what is still in the other format.  Reached through
   * set_profile();  separate so a stopped run can be resumed without
   * touching the marker again. */
  int convert_tree(const DoutPrefixProvider* dpp, optional_yield y,
		   nsfs::convert_cb_t* cb,
		   nsfs::ConvertProgress* progress);

  /* Whether a client write may change the directory object stored at
   * `dir`.  Returns 0 unless the object is still in NooBaa's format:
   * then -ERR_SERVICE_UNAVAILABLE in a bucket whose adoption has not
   * reached it -- the write waits for the conversion and converts
   * nothing (DECISIONS.md 2026-10-21) -- and -EOPNOTSUPP in a bucket
   * that is not being converted. */
  int admit_write(const DoutPrefixProvider* dpp, optional_yield y,
		  nsfs::Directory* dir, const std::string& key);

private:
  /* Re-stage the uploads still in the other format.  Part of
   * convert_tree();  separate because staging is a different shape
   * from the object tree and shares nothing with the walk. */
  int convert_uploads(const DoutPrefixProvider* dpp, optional_yield y,
		      nsfs::convert_cb_t* cb,
		      nsfs::ConvertProgress& progress);
public:

  /* Write the extensions attribute.  Reached through set_profile(),
   * and directly at creation. */
  int mark_extensions(const DoutPrefixProvider* dpp, uint32_t version);

  /* Only the `unmarked-bucket` test hint can call this;  see
   * UnmarkPasskey. */
  int unmark_extensions(const DoutPrefixProvider* dpp, const UnmarkPasskey&);

  /* Write the converting marker, and remove it.
   *
   * Two functions rather than one with a flag.  The flag was named
   * `from_noobaa`, which reads as the source format -- and the
   * attribute's *value* is exactly that, so `mark_converting(dpp,
   * false)` read as "converting, but not from NooBaa" when it meant
   * "stop converting".  The value dimension is real and the set/clear
   * dimension is real;  one bool cannot carry both.
   *
   * There is one source format, so `mark_converting` names it in the
   * body rather than taking it.  A second format is when a parameter
   * earns its place.
   *
   * Written by the upgrade leaving base and removed when it finishes;
   * nothing else may touch it, because it is what says a foreign
   * reader is still needed. */
  int mark_converting(const DoutPrefixProvider* dpp);
  int clear_converting(const DoutPrefixProvider* dpp);
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl,
		      optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider *dpp, optional_yield y,
			 const bucket_index_layout_generation& idx_layout,
			 int shard_id, std::string* bucket_ver, std::string* master_ver,
			 std::map<RGWObjCategory, RGWStorageStats>& stats,
			 std::string* max_marker = nullptr,
			 bool* syncstopped = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider *dpp,
			       const bucket_index_layout_generation& idx_layout,
			       int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx) override;
  virtual int sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                               RGWBucketEnt* ent) override;
  virtual int check_bucket_shards(const DoutPrefixProvider* dpp,
                                  uint64_t num_objs, optional_yield y) override;
  virtual int chown(const DoutPrefixProvider* dpp,
                    const rgw_owner& new_owner,
                    const std::string& new_owner_name,
                    optional_yield y) override;

  /* What a bucket chown changes (DECISIONS.md 2026-10-11).
   *
   * `metadata` is the owner, the bucket ACL and the listing cache, and
   * nothing on disk:  no file's uid, gid or mode moves.  That is what
   * NooBaa's `bucket update --owner` does, and what every RGW path
   * gets -- the SAL chown() above calls it.  A new owner whose POSIX
   * identity differs is not rejected;  the files are left to the
   * operator, as NooBaa leaves them.
   *
   * `posix` is the same and then gives the new owner's POSIX identity
   * every file and directory under the bucket, the bucket directory
   * excepted, which is the gateway's (NSFS_IMPERSONATION.md 8a).  It
   * rejects, before changing anything, a new owner with no identity
   * row or a directory-backed one.  Nothing calls it yet:  it is the
   * full chown with a model shift, kept where the next reader finds it
   * beside the one in use. */
  enum class ChownMode { metadata, posix };
  int chown(const DoutPrefixProvider* dpp, const rgw_owner& new_owner,
	    const std::string& new_owner_name, ChownMode mode,
	    optional_yield y);
  virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive,
                       ceph::real_time mtime, optional_yield y) override;
  virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
  /* whether this bucket's directory exists for it or predates it, and
   * what remove() does when it predates it -- see OWNS_STORAGE_XATTR */
  bool owns_storage(const DoutPrefixProvider* dpp);
  /* Record it:  absent for a bucket that owns its directory, "0" for one
   * whose directory predates it.  The one place this fact is kept
   * (DECISIONS.md 2026-10-13). */
  int set_owns_storage(const DoutPrefixProvider* dpp, bool owns);
  int release_nsfs_state(const DoutPrefixProvider* dpp);
  virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
  virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y) override;
  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, uint32_t max_entries, bool* is_truncated,
			 RGWUsageIter& usage_iter, std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;
  virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
  virtual int check_index(const DoutPrefixProvider *dpp, optional_yield y,
                          std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                          std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
  virtual int rebuild_index(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout) override;
  virtual int purge_instance(const DoutPrefixProvider* dpp, optional_yield y) override;

  virtual std::unique_ptr<Bucket> clone() override {
    return std::make_unique<NSFSBucket>(*this);
  }

  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				const rgw_owner& authenticated_user,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
  virtual int list_multiparts(const DoutPrefixProvider *dpp,
			      const std::string& prefix,
			      std::string& marker,
			      const std::string& delim,
			      const int& max_uploads,
			      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
			      std::map<std::string, bool> *common_prefixes,
			      bool *is_truncated, optional_yield y,
			      const rgw_owner& authenticated_user) override;
  virtual int abort_multiparts(const DoutPrefixProvider* dpp,
			       CephContext* cct, optional_yield y) override;

  /* Internal APIs */
  /* abort_multiparts() as `authenticated_user`, each abort through the access
   * model;  no_authenticated_user is the gateway, raised after a
   * denial, as abort_multiparts() is */
  int abort_multiparts_as(const DoutPrefixProvider* dpp, CephContext* cct,
			  optional_yield y,
			  const rgw_owner& authenticated_user);
  /* The bucket directory made by whoever had to be able to make it
   * (DECISIONS.md 2026-10-24):  the gateway in the base path, `creator`
   * under a location the account owns. */
  int make_dir(const DoutPrefixProvider* dpp, optional_yield y,
	       bool* existed, const rgw_owner& creator);
  /* An owned bucket directory removed with `id` acting where the
   * authenticated user acts (remove()):  everything in it, and its entry in a
   * location the account owns;  the gateway removes its entry from the
   * base path. */
  int remove_dir_as(const DoutPrefixProvider* dpp, optional_yield y,
		    const nsfs::FSIdentity& id,
		    const nsfs::AccessStrategy* model, bool delete_children);
  /* `creator` is who the directory is given to (raise site 4), or
   * no_authenticated_user where no request made it */
  int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed,
	     const rgw_owner& creator);
  nsfs::Directory* get_dir() { return dir.get(); }
  int get_dir_fd(const DoutPrefixProvider *dpp) { dir->open(dpp); return dir->get_fd(); }
  /* TODO dang Escape the bucket name for file use */
  std::string get_fname();
  std::optional<std::string> get_ns() { return ns; }
  int rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj);

  /* enumerate all entries by callback, in any order */
  int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, nsfs::fill_cache_cb_t& cb);

  /* The owner of an object here that records none, with the display
   * name RGW gives it:  the bucket's owner.  NooBaa returns the
   * bucket's owner for every object, and an adopted bucket is
   * BucketOwnerEnforced (DECISIONS.md 2026-10-11).  Looked up once per
   * handle;  a display name that cannot be looked up is left empty. */
  const ACLOwner& default_object_owner(const DoutPrefixProvider* dpp,
				       optional_yield y);

  static MDB_cmp_func* lmdb_cmp();

private:
  int write_attrs(const DoutPrefixProvider *dpp, optional_yield y);
}; /* NSFSBucket */


class NSFSObject : public StoreObject {
public:
private:
  NSFSDriver* driver;
  RGWAccessControlPolicy acls;
  std::unique_ptr<nsfs::FSEnt> ent;
  std::vector<std::unique_ptr<nsfs::Directory>> dir_chain;
  std::map<std::string, int64_t> parts;
  std::unique_ptr<rgw::sal::Bucket> pinned_bucket;
  std::string dm_version_id;
  /* the last stat() found a directory under a key without the trailing
   * '/', which has no object (stat()) */
  bool names_directory{false};

  /* How this object's entry is to be reached, held until an entry
   * exists to apply it to.
   *
   * stat() both creates the entry and opens it, so a caller cannot
   * set this on the entry afterwards -- by then the descriptor
   * exists and its access mode is fixed.  The write path sidesteps
   * the problem by calling make_ent() itself and setting it in
   * between, which a read cannot do:  make_ent() resolves with
   * create_dirs. */
  nsfs::FSIdentity pending_identity;
  bool pending_read_only{false};

  void apply_pending_access() {
    if (! ent) {
      return;
    }
    ent->set_read_only(pending_read_only);
    if (pending_identity.active()) {
      ent->set_identity(pending_identity);
      ent->set_access_model(driver->get_access_strategy());
      /* and the key's directories, which a delete prunes */
      for (auto& d : dir_chain) {
	d->set_identity(pending_identity);
	d->set_access_model(driver->get_access_strategy());
      }
    }
  }

public:
  /* Say how the entry is to be opened before anything opens it.
   *
   * An inactive identity means the gateway's own credentials.
   * read_only is what a read op always wants:  asking for write
   * access it does not need is the difference between serving an
   * object readable through a supplementary group and the open failing
   * with EACCES. */
  void set_access(const nsfs::FSIdentity& id, bool ro) {
    pending_identity = id;
    pending_read_only = ro;
    apply_pending_access();
  }

  /* The strategies this object's bucket is in.
   *
   * Through the bucket, not the driver:  the driver's are nsfs's, and a
   * base bucket's objects are in NooBaa's format.  They fall back to
   * the driver's for an object with no bucket, which is what a few
   * internal paths construct. */
  nsfs::XattrStrategy* xattr_strategy() const;
  /* the bucket's fallback, null unless it is mid-conversion */
  nsfs::XattrStrategy* xattr_fallback() const;
  nsfs::PathStrategy* path_strategy() const;

  struct NSFSReadOp : ReadOp {
    NSFSObject* source;
    int64_t part_ofs{0};

    NSFSReadOp(NSFSObject* _source, const rgw_owner& _auth_user) :
      source(_source), auth_user(_auth_user) {}
    virtual ~NSFSReadOp() = default;

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int read(int64_t ofs, int64_t left, bufferlist& bl, optional_yield y,
		     const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			 bufferlist& dest, optional_yield y) override;

    /* Who this op is for, from get_read_op().  The identity is resolved
     * at first I/O rather than here, so an operation that never reads a
     * byte costs no registration.  No personality is held across the
     * operation:  each submission looks one up on the thread it runs
     * on. */
    rgw_owner auth_user;
    nsfs::FSIdentity identity;
    /* resolve who this op runs as, into `identity`.  0 or -errno;
     * -ENODATA means impersonation is off and the caller proceeds as
     * before. */
    int pin_personality(const DoutPrefixProvider* dpp, optional_yield y);
  };

  struct NSFSDeleteOp : DeleteOp {
    NSFSObject* source;
    /* who asked, from get_delete_op(), for the writes a delete makes
     * inside `.versions` */
    rgw_owner auth_user;

    NSFSDeleteOp(NSFSObject* _source, const rgw_owner& _auth_user) :
      source(_source), auth_user(_auth_user) {}
    virtual ~NSFSDeleteOp() = default;

    /* A delete of a current version in a versioned bucket *writes*:
     * it links the current name into `.versions` and unlinks it from
     * the bucket directory, against an object an identity wrote.  As
     * the gateway that link fails with EPERM under
     * fs.protected_hardlinks, so the authenticated user is what those
     * two calls need (work-plan stage N-D).
     *
     * Not the promote that may follow them.  That publishes as the
     * version file's own owner (§5) and must stay outside any
     * `with_identity` here:  `run_as` rejects a call as one identity
     * on a thread already acting as another. */
    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
  };

  NSFSObject(NSFSDriver *_dr, const rgw_obj_key& _k)
    : StoreObject(_k),
    driver(_dr),
    acls()
    {}

  NSFSObject(NSFSDriver* _driver, const rgw_obj_key& _k, Bucket* _b) :
    StoreObject(_k, _b),
    driver(_driver),
    acls()
    {}

  NSFSObject(const NSFSObject& _o) :
    StoreObject(_o),
    driver(_o.driver)
  {}

  virtual ~NSFSObject() { close(); }

  void pin_bucket(std::unique_ptr<rgw::sal::Bucket> b) {
    pinned_bucket = std::move(b);
    set_bucket(pinned_bucket.get());
  }

  virtual int delete_object(const DoutPrefixProvider* dpp,
			    optional_yield y,
			    uint32_t flags,
			    std::list<rgw_obj_index_key>* remove_objs,
			    RGWObjVersionTracker* objv) override;
  virtual int rename(const DoutPrefixProvider* dpp, optional_yield y,
  		     rgw::sal::Bucket* dest_bucket,
  		     const rgw_obj_key& dest_key, uint32_t flags,
		     const rgw_owner& authenticated_user) override;

  virtual int copy_object(const ACLOwner& owner,
               const rgw_user& remote_user,
               const rgw_owner& authenticated_user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
               boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               rgw::sal::DataProcessorFactory* dp_factory,
               const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy& acl) override { acls = acl; return 0; }

  /** If multipart, enumerate (a range [marker..marker+[min(max_parts, parts_count-1)] of) parts of the object */
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int max_parts, int marker, int* next_marker,
			 bool* truncated, list_parts_each_t&& each_func,
			 optional_yield y) override;

  FSIOResult get_fsio_handle(const DoutPrefixProvider* dpp,
			     const rgw_owner& authenticated_user,
			     uint32_t flags = FSIOObject::OPEN_FLAG_NONE,
			     const FSIOCreateSpec* spec = nullptr) override;

  /* whether this object's bucket carries the extensions FSIO needs */
  int fsio_allowed(const DoutPrefixProvider* dpp);

  int stat_fsio_view(const DoutPrefixProvider* dpp, struct stat* st,
		     Attrs* attrs, uint32_t flags) override;

  /* resolve this object's parent directory, opening the chain from the
   * bucket root;  see the definition for why ent->get_parent() will not
   * do for a nested key */
  int resolve_parent_dir(const DoutPrefixProvider* dpp, bool create_dirs,
			 std::vector<std::unique_ptr<nsfs::Directory>>& chain,
			 nsfs::Directory*& dir, std::string& leaf);

  bool is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                         const ceph::real_time& obj_mtime) override;

  virtual int load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh = true) override;
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
			    Attrs* delattrs, optional_yield y, uint32_t flags,
			    const rgw_owner& authenticated_user) override;
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp) override;
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
			       optional_yield y, const DoutPrefixProvider* dpp,
			       const rgw_owner& authenticated_user,
			       uint32_t flags = rgw::sal::FLAG_LOG_OP) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
			       optional_yield y, const rgw_owner& authenticated_user) override;
  virtual bool is_expired() override;
  virtual void gen_rand_obj_instance_name() override;
  virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp, optional_yield y,
						       const std::string& lock_name) override;
  virtual int transition(Bucket* bucket,
			 const rgw_placement_rule& placement_rule,
			 const real_time& mtime,
			 uint64_t olh_epoch,
			 const DoutPrefixProvider* dpp,
			 optional_yield y,
                         uint32_t flags) override;
  virtual int transition_to_cloud(Bucket* bucket,
			 rgw::sal::PlacementTier* tier,
			 rgw_bucket_dir_entry& o,
			 std::set<std::string>& cloud_targets,
			 CephContext* cct,
			 bool update_object,
			 const DoutPrefixProvider* dpp,
			 optional_yield y) override;
  virtual int restore_obj_from_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   CephContext* cct,
			   std::optional<uint64_t> days,
		           bool& in_progress,
			   uint64_t& size,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) override;
  virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) override;
  virtual int swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual std::unique_ptr<ReadOp> get_read_op(const rgw_owner& authenticated_user) override;
  virtual std::unique_ptr<DeleteOp> get_delete_op(const rgw_owner& authenticated_user) override;
  virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
				    const std::set<std::string>& keys,
				    Attrs* vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key,
				  bufferlist& val, bool must_exist, optional_yield y) override;
  virtual int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new NSFSObject(*this));
  }

  nsfs::FSEnt* get_fsent() { return ent.get(); }
  int open(const DoutPrefixProvider *dpp, optional_yield y, bool create = false,
	   bool temp_file = false);
  int close();
  int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y);
  int write_attrs(const DoutPrefixProvider* dpp, optional_yield y,
		  const std::vector<std::string>* rmattrs = nullptr,
		  int use_fd = -1);
  int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, bool excl = false);
  std::string gen_temp_fname();
  const std::string get_fname(bool use_version);
  bool check_exists(const DoutPrefixProvider* dpp) { stat(dpp); return state.exists; }
  int get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner);
  int copy(const DoutPrefixProvider *dpp, optional_yield y, NSFSBucket *sb,
           NSFSBucket *db, NSFSObject *dobj, const nsfs::CopyHints& hints);
  /* Whether the access model lets the gateway read a source the
   * filesystem denies this authenticated user.  Resolved here because this is
   * where the driver and the object are both in hand. */
  bool copy_source_as_gateway() const;
  int fill_cache(const DoutPrefixProvider *dpp, optional_yield y, nsfs::fill_cache_cb_t& cb);
  int stat(const DoutPrefixProvider *dpp);
  int make_ent(nsfs::ObjectType type);
  /* Before a change to this object:  whether the write may proceed,
   * which a directory object still in NooBaa's format does not until
   * the bucket's adoption reaches it (NSFSBucket::admit_write()).  Must
   * be called under the bucket's mutation guard. */
  int admit_write(const DoutPrefixProvider* dpp, optional_yield y);

  /* Put the authenticated user on the entry, so the open and the
   * attribute write that follow are that user's rather than the
   * gateway's.
   *
   * An attribute write opens the object O_RDWR and sets a `user.*`
   * name on it, and both are tested against the calling thread:  as
   * the gateway, on an identity's 0660 object, both fail.
   * That used to pass silently and now returns (`09bf3d2b9ea`), so
   * PutObjectTagging and friends answered 403.
   *
   * An empty `who` is not a failure.  It is what
   * `rgw::sal::no_authenticated_user` means -- lifecycle, restore, a
   * chown -- and those run as the gateway by design. */
  int pin_identity_for_attrs(const DoutPrefixProvider* dpp,
			     const rgw_owner& who, optional_yield y);

  /* Run an attribute operation, and where the filesystem denied the
   * authenticated user an operation S3 had authorised, run it again as the
   * gateway with a raise.
   *
   * This is the whole of `rgw_nsfs_cross_account_permission`, and it
   * is off by default.  Off, an attribute write or read must pass
   * both S3 and the filesystem, which is NooBaa's model:  they
   * perform every operation as the requesting account, and their IAM
   * users and roles all carry their account's uid, so S3 and POSIX
   * cannot disagree inside an account.  Cross-account sharing is then
   * arranged in the filesystem, with a supplementary group (§6.2).
   *
   * On, an S3 grant to another account takes effect on the object's
   * tags and ACL -- the gateway's own records about the object.  Not
   * on its bytes:  no value of this setting makes the gateway read
   * out or overwrite data for an identity the filesystem denies.
   *
   * The retry is the promote's last rung in miniature, and for the
   * same reason:  attempt as the identity, and raise only where that
   * is denied, so a tree that needs no raise never pays for one. */
  int with_cross_account_retry(const DoutPrefixProvider* dpp,
			       nsfs::AccessClass what,
			       std::function<int(int use_fd)> fn);

 class NSFSFSIOObject : public FSIOObject {
  private:
   NSFSObject* src_obj;
   NSFSDriver* driver;
   int shadow_fd{-1};
   int shadow_dir_fd{-1};
   int parent_fd{-1};
   std::string shadow_name;
   std::string leaf_name;
   bool ephemeral{false};
   std::vector<std::unique_ptr<nsfs::Directory>> dir_chain;
   /* A write handle's slot, held for the handle's whole life.
    *
    * Opening one is the mutation;  the writes through it are parts of
    * that mutation and not separate ones, and releasing the handle is
    * what retires it.  So the gate is entered at open and left at
    * destruction, and a bucket drains as its write handles are
    * released.  Empty on a read handle, which mutates nothing. */
   std::optional<NSFSMutationGuard> gate;
   /* who the handle is for (n plan D4a):  librgw's mount user, pinned
    * at get_fsio_handle() and held for the handle's life;  every call
    * on the tree runs as it, and as the gateway where it is inactive */
   nsfs::FSIdentity identity;
   /* the same user, for the hints its copies carry:  set whether or
    * not impersonation is, where `identity` is not */
   rgw_owner authenticated_user;

   friend class NSFSObject;

  protected:
   NSFSFSIOObject(NSFSObject* _obj, NSFSDriver* _drv, bool _ephemeral,
		  std::optional<NSFSMutationGuard>&& _gate = std::nullopt)
     : src_obj(_obj), driver(_drv), ephemeral(_ephemeral),
       gate(std::move(_gate)) {}

  public:
    virtual int preadv(const struct iovec* iov, int iovcnt,
		       uint64_t ofs, uint64_t* bytes_read,
		       uint32_t flags) override;
    virtual int pwritev(const struct iovec* iov, int iovcnt,
			uint64_t ofs, uint64_t* bytes_written,
			uint32_t flags) override;
    virtual int commit(const DoutPrefixProvider* dpp, uint32_t flags) override;
    virtual int publish(const DoutPrefixProvider* dpp, uint32_t flags) override;
    virtual int reclone(const DoutPrefixProvider* dpp, uint32_t flags) override;
    virtual int close(const DoutPrefixProvider* dpp, uint32_t flags) override;
    virtual int discard(const DoutPrefixProvider* dpp, uint32_t flags) override;
    virtual int ftruncate(const DoutPrefixProvider* dpp, uint64_t size,
			  uint32_t flags) override;

    virtual int fstat(struct stat* st, uint32_t flags) override;
    virtual int fgetattr(const DoutPrefixProvider* dpp,
			  const std::string& name, bufferlist& dest,
			  uint32_t flags) override;
    virtual int fsetattr(const DoutPrefixProvider* dpp,
			  const std::string& name, const bufferlist& val,
			  uint32_t flags) override;
    virtual int fgetattrs(const DoutPrefixProvider* dpp,
			   Attrs& attrs, uint32_t flags) override;
    virtual int fsetattrs(const DoutPrefixProvider* dpp,
			   Attrs& attrs, uint32_t flags) override;
    virtual int fremovexattr(const DoutPrefixProvider* dpp,
			      const std::string& name, uint32_t flags) override;

    virtual ~NSFSFSIOObject() override;
  }; /* NSFSFSIOObject */

protected:
  int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y);
private:
  int get_cur_version(const DoutPrefixProvider *dpp, rgw_obj_key &key);
};

/* NSFSMPObj moved to mpu_records.h, beside NSFSUploadPartInfo and for
 * the same reason:  an upload's record is the staging layout's, and a
 * strategy has to be able to read what its own layout wrote. */

/* NSFSUploadPartInfo moved to part_info.h:  a part's record is the
 * staging layout's, so MPUStrategy has to be able to read it. */

class NSFSMultipartUpload;

class NSFSMultipartPart : public StoreMultipartPart {
protected:
  NSFSUploadPartInfo info;
  NSFSMultipartUpload* upload;
  std::unique_ptr<nsfs::File> part_file;

public:
  NSFSMultipartPart(NSFSMultipartUpload* _upload) :
    upload(_upload) {}
  virtual ~NSFSMultipartPart() = default;

  virtual uint32_t get_num() { return info.num; }
  virtual uint64_t get_size() {
    if (info.size) return info.size;
    return part_file ? part_file->get_size() : 0;
  }
  virtual const std::string& get_etag() { return info.etag; }
  virtual ceph::real_time& get_mtime() { return info.mtime; }
  virtual const std::optional<rgw::cksum::Cksum>& get_cksum() {
    return info.cksum;
  }

  int load(const DoutPrefixProvider* dpp, optional_yield y, NSFSDriver* driver,
	   rgw_obj_key& key);

  friend class NSFSMultipartUpload;
};

class NSFSMultipartUpload : public StoreMultipartUpload {
protected:
  NSFSDriver* driver;
  NSFSMPObj mp_obj;
  ceph::real_time mtime;
  std::unique_ptr<rgw::sal::NSFSBucket> shadow;
  /* who the upload's operations are for, from get_multipart_upload() */
  rgw_owner auth_user;
  /* whom work no request asked for is done for:  lifecycle's abort */
  rgw_owner on_behalf_of;
  nsfs::FSIdentity identity;

public:
  NSFSMultipartUpload(NSFSDriver* _driver, Bucket* _bucket, const std::string& _oid,
		       std::optional<std::string> _upload_id, ACLOwner _owner,
		       ceph::real_time _mtime, const rgw_owner& _auth_user) :
    StoreMultipartUpload(_bucket), driver(_driver),
    mp_obj(driver, _oid, _upload_id, _owner), mtime(_mtime),
    auth_user(_auth_user) {}
  virtual ~NSFSMultipartUpload() = default;

  virtual void set_on_behalf_of(const rgw_owner& owner) override {
    on_behalf_of = owner;
  }

  /* complete() resolves the authenticated user and makes the object
   * as it:  the assembled file is created, or a data file linked in,
   * and its attributes written, as the identity.  Parts are written as their
   * uploader by the part writer, as NooBaa writes them.  init() makes
   * staging and the meta object as the uploader, and abort() removes
   * staging as the authenticated user (DECISIONS.md 2026-10-23);  ListParts
   * stays the gateway's (DECISIONS.md 2026-10-15). */

  virtual const std::string& get_meta() const override { return mp_obj.meta; }
  virtual const std::string& get_key() const override { return mp_obj.oid; }
  virtual const std::string& get_upload_id() const override { return mp_obj.upload_id; }
  virtual const ACLOwner& get_owner() const override { return mp_obj.owner; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;

  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated, optional_yield y,
			 bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y) override;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj,
		       prefix_map_t& processed_prefixes,
           const char *if_match = nullptr,
           const char *if_nomatch = nullptr) override;
  virtual int cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
                                     CephContext *cct, optional_yield y,
                                     const rgw_obj& obj,
                                     std::list<rgw_obj_index_key>& remove_objs,
                                     prefix_map_t& processed_prefixes) override;
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y,
		       rgw_placement_rule** rule, rgw::sal::Attrs* attrs) override;

  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* _head_obj,
			  const ACLOwner& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str,
			  const rgw_owner& authenticated_user) override;

  NSFSBucket* get_shadow() { return shadow.get(); }

  /* the stride this upload runs at, or nothing;  see the definition.
   * Public because the driver-hint read-back asks it:  whether an
   * upload established a stride is the one fact a test cannot see
   * through S3, and it is what makes the establishment conditions
   * assertable in both polarities. */
  std::optional<uint64_t> established_stride(const DoutPrefixProvider* dpp,
					     optional_yield y);
  int load_for_hint(const DoutPrefixProvider* dpp) { return load(dpp); }

private:
  std::string get_fname();

  /* The staging layout this upload's bucket is in.  Resolved through
   * the bucket rather than the driver, because which layout a tree
   * stages in is a property of its format. */
  nsfs::MPUStrategy* mpu_strategy() const;
  /* and the other two, for the same reason:  the object this upload
   * completes into is written in the bucket's format, not the
   * driver's */
  nsfs::XattrStrategy* xattr_strategy() const;
  /* the bucket's fallback, null unless it is mid-conversion */
  nsfs::XattrStrategy* xattr_fallback() const;
  nsfs::PathStrategy* path_strategy() const;

  /* The directory holding staging directories, where the format keeps
   * them somewhere other than the bucket.  Held because the staging
   * directory keeps a raw parent pointer into it. */
  std::unique_ptr<nsfs::Directory> staging_root_dir;
  int load(const DoutPrefixProvider *dpp, bool create=false);
  /* this upload's staging directory, as `identity`;  0 or -errno */
  int remove_staging(const DoutPrefixProvider* dpp, optional_yield y);
};

class NSFSAtomicWriter : public StoreWriter {
private:
  NSFSDriver* driver;
  const ACLOwner& owner;
  const rgw_placement_rule *ptail_placement_rule;
  uint64_t olh_epoch;
  const std::string& unique_tag;
  NSFSObject* obj;
  QD2PendingWrite pending_write;
  optional_yield yield;
  rgw_owner auth_user;
  nsfs::FSIdentity identity;
  /* who uring_write's transfers run as;  declared before it, so it
   * outlives it */
  nsfs::IdentityPersonality uring_identity{identity};
  std::unique_ptr<UringWriteWindow> uring_write;

public:
  int pin_personality(const DoutPrefixProvider* dpp, optional_yield y);

  NSFSAtomicWriter(const DoutPrefixProvider *dpp,
                    optional_yield y,
		    rgw::sal::Object* _head_obj,
                    NSFSDriver* _driver,
                    const ACLOwner& _owner,
                    const rgw_placement_rule *_ptail_placement_rule,
                    uint64_t _olh_epoch,
                    const std::string& _unique_tag,
                    const rgw_owner& _auth_user) :
    StoreWriter(dpp, y),
    driver(_driver),
    owner(_owner),
    ptail_placement_rule(_ptail_placement_rule),
    olh_epoch(_olh_epoch),
    unique_tag(_unique_tag),
    obj(static_cast<NSFSObject*>(_head_obj)),
    yield(y),
    auth_user(_auth_user) {}
  virtual ~NSFSAtomicWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
		       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
		       ceph::real_time delete_at,
		       const char *if_match, const char *if_nomatch,
		       const std::string *user_data,
		       rgw_zone_set *zones_trace, bool *canceled,
		       const req_context& rctx,
                       uint32_t flags) override;
};

class NSFSMultipartWriter : public StoreWriter {
private:
  rgw_owner auth_user;
  nsfs::FSIdentity identity;
  /* who uring_write's transfers run as;  declared before it, so it
   * outlives it */
  nsfs::IdentityPersonality uring_identity{identity};
  NSFSDriver* driver;
  /* the upload's staging layout, resolved once by get_writer();  this
   * writer has no bucket of its own to ask */
  nsfs::MPUStrategy* mpu_strat;
  const ACLOwner& owner;
  const rgw_placement_rule *ptail_placement_rule;
  uint64_t part_num;
  /* where this part's bytes begin in the file it is written to.  Zero
   * for a layout giving each part its own file;  MPUStrategy decides
   * (see part_target()). */
  uint64_t base_offset;
  /* bytes stored for this part, counted as they are written.
   *
   * NOT accounted_size, which complete() is handed:  that is the
   * pre-filter length the client sent, which is what ListParts must
   * report and what quota bills.  Compression and AEAD sit between the
   * op layer and this writer, so the two differ whenever one of them is
   * active, and only the stored count can place a part. */
  uint64_t high_water{0};
  /* does base_offset point into a file other parts also write?  From
   * MPUStrategy::part_target;  recorded so assembly knows where to read
   * this part from. */
  bool shared_target{false};
  /* how much this part may write at base_offset before it would run
   * into the next part's region.  Unset where the part has a file to
   * itself and nothing bounds it. */
  std::optional<uint64_t> extent;
  /* set once this part has been moved out of the shared file */
  bool diverted{false};

  /* Move this part out of the shared file and into its own.
   *
   * Called when a write would cross the extent.  S3 does not require
   * parts to be equal -- only that every part but the last reaches a
   * minimum -- so a part longer than the stride is legal, and the
   * stride is an inference from part 1 with nothing behind it.
   * Writing past the extent would land in part K+1's region, and the
   * damage is invisible afterwards:  each record still describes where
   * its part was meant to go. */
  int divert(uint64_t written);
  /* Move this part into the file its own length belongs in.
   *
   * Asked of the layout, which is the only thing that knows whether a
   * file's identity depends on the size it holds;  see
   * MPUStrategy::relocation_target().  A no-op for the nsfs layout. */
  int relocate(optional_yield y);
  /* the upload this part belongs to, as the client knows it.  The
   * injection map is keyed by this rather than by the internal meta,
   * so a test can arm exactly the upload it created. */
  std::string upload_id;
  /* the bucket the part is for, which the mutation gate asks;  the
   * staging directory is the shadow bucket's, not this one */
  NSFSBucket* bucket{nullptr};
  std::unique_ptr<nsfs::Directory> upload_dir;
  /* the part's RECORD -- part-NNNNN -- which carries its attributes.
   * Always this file, whatever the layout, because complete() writes
   * the record and only process() writes bytes. */
  std::unique_ptr<nsfs::File> part_file;
  /* the file the BYTES go to, when that is not the record.  Null in the
   * one-file-per-part layout, where the two coincide;  the shared data
   * file otherwise, in which case the record file stays empty and its
   * size means nothing. */
  std::unique_ptr<nsfs::File> shared_file;

  /* where this part's bytes are written */
  nsfs::File* data_file() {
    return shared_file ? shared_file.get() : part_file.get();
  }
  file::listing::MultipartCacheKey mp_cache_key;
  QD2PendingWrite pending_write;
  optional_yield yield;
  std::unique_ptr<UringWriteWindow> uring_write;

public:
  int pin_personality(const DoutPrefixProvider* dpp, optional_yield y);

  NSFSMultipartWriter(const DoutPrefixProvider *dpp,
                    optional_yield y,
		    NSFSBucket* _shadow_bucket,
                    rgw_obj_key& _key,
                    NSFSDriver* _driver,
                    const ACLOwner& _owner,
                    const rgw_placement_rule *_ptail_placement_rule,
                    uint64_t _part_num,
		    nsfs::MPUStrategy* _mpu_strat,
		    file::listing::MultipartCacheKey&& _mp_cache_key,
		    const rgw_owner& _auth_user,
		    uint64_t _base_offset = 0,
		    bool _shared_target = false,
		    const std::string& _shared_name = std::string(),
		    const std::string& _upload_id = std::string(),
		    std::optional<uint64_t> _extent = std::nullopt,
		    NSFSBucket* _bucket = nullptr) :
    StoreWriter(dpp, y),
    auth_user(_auth_user),
    driver(_driver),
    mpu_strat(_mpu_strat),
    owner(_owner),
    ptail_placement_rule(_ptail_placement_rule),
    part_num(_part_num),
    base_offset(_base_offset),
    shared_target(_shared_target),
    extent(_extent),
    upload_id(_upload_id),
    bucket(_bucket),
    upload_dir(_shadow_bucket->get_dir()->clone()),
    part_file(std::make_unique<nsfs::File>(
		_driver->get_path_strategy()->object_name(_key, false),
		upload_dir.get(), _driver->ctx())),
    mp_cache_key(std::move(_mp_cache_key)),
    yield(y)
  {
    upload_dir->open(dpp);
    if (!_shared_name.empty()) {
      shared_file = std::make_unique<nsfs::File>(_shared_name,
						 upload_dir.get(),
						 _driver->ctx());
    }
  }
  virtual ~NSFSMultipartWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
		       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
		       ceph::real_time delete_at,
		       const char *if_match, const char *if_nomatch,
		       const std::string *user_data,
		       rgw_zone_set *zones_trace, bool *canceled,
		       const req_context& rctx,
                       uint32_t flags) override;

};

class MPNSFSSerializer : public StoreMPSerializer {
  NSFSObject* obj;

public:
  MPNSFSSerializer(const DoutPrefixProvider *dpp, NSFSDriver* driver, NSFSObject* _obj, const std::string& lock_name) : obj(_obj) {}

  virtual int try_lock(const DoutPrefixProvider *dpp, ceph::timespan dur, optional_yield y) override;
  virtual int unlock(const DoutPrefixProvider* dpp, optional_yield y) override;
};

class LCNSFSSerializer : public StoreLCSerializer {
public:
  LCNSFSSerializer(NSFSDriver* driver, const std::string& oid, const std::string& lock_name, const std::string& cookie) {}

  virtual int try_lock(const DoutPrefixProvider* dpp, ceph::timespan dur, optional_yield y) override { return 0; }
  virtual int unlock(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
};

class NSFSLifecycle : public Lifecycle {
  NSFSDriver* driver;

public:
  NSFSLifecycle(NSFSDriver* _driver) : driver(_driver) {}
  virtual ~NSFSLifecycle() = default;

  virtual int get_entry(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& oid, const std::string& marker,
                        LCEntry& entry) override;
  virtual int get_next_entry(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const std::string& marker,
                             LCEntry& entry) override;
  virtual int set_entry(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& oid, const LCEntry& entry) override;
  virtual int list_entries(const DoutPrefixProvider* dpp, optional_yield y,
                           const std::string& oid, const std::string& marker,
                           uint32_t max_entries,
                           std::vector<LCEntry>& entries) override;
  virtual int rm_entry(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, const LCEntry& entry) override;
  virtual int get_head(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, LCHead& head) override;
  virtual int put_head(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, const LCHead& head) override;

  virtual std::unique_ptr<LCSerializer> get_serializer(const std::string& lock_name,
                                                       const std::string& oid,
                                                       const std::string& cookie) override;
};

} } // namespace rgw::sal
