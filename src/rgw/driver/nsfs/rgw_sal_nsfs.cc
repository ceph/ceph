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

#include "rgw_sal_nsfs.h"
#include "rgw_rest_user.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_s3_filter.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include "rgw_mime.h"
#include "rgw_multi.h"
#include "include/scope_guard.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/errno.h"
#include "rgw_lc.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

template <typename T>
static bool decode_raw_attr(rgw::sal::Attrs& attrs, const char* name, T& val) {
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    return false;
  }
  bufferlist bl = it->second;
  try {
    auto it = bl.cbegin();
    decode(val, it);
  } catch (buffer::error&) {
    return false;
  }
  return true;
}

template <typename T>
static void encode_attr(rgw::sal::Attrs& attrs, const char* name, const T& val) {
  bufferlist bl;
  encode(val, bl);
  attrs[name] = std::move(bl);
}

namespace rgw { namespace sal {

using namespace nsfs;

const int64_t READ_SIZE = 128 * 1024;

static const std::string NSFS_XATTR_PREFIX = "user.nsfs.";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs.rgw.";
static const std::string RGW_ATTR_PFX = "user.rgw.";

static inline std::string make_xattr_name(const std::string& key) {
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}

static inline bool parse_xattr_name(const std::string& xattr, std::string& key) {
  if (xattr.compare(0, NSFS_RGW_XATTR_PREFIX.size(), NSFS_RGW_XATTR_PREFIX) == 0) {
    key = RGW_ATTR_PFX + xattr.substr(NSFS_RGW_XATTR_PREFIX.size());
    return true;
  }
  if (xattr.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = xattr.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }
  return false;
}

#define RGW_NSFS_ATTR_BUCKET_INFO "bucket_info"
#define RGW_NSFS_ATTR_MPUPLOAD "mp_upload"
#define RGW_NSFS_ATTR_OBJECT_TYPE "object_type"
#define RGW_NSFS_ATTR_MULTIPART_PART_COUNT "multipart_part_count"
#define RGW_NSFS_ATTR_MULTIPART_PART_SIZES "multipart_part_sizes"

#define RGW_NSFS_ATTR_VERSION_ID "version_id"
#define RGW_NSFS_ATTR_DELETE_MARKER "delete_marker"
#define RGW_NSFS_ATTR_NON_CURRENT_TS "non_current_timestamp"

static const std::string HIDDEN_VERSIONS_PATH = ".versions";
static const std::string NULL_VERSION_ID = "null";

static constexpr int NSFS_VERSION_RETRIES = 5;
static const std::string VERSIONS_LOCKFILE = ".versions/.lock";

/* version locking is now handled by FSStrategy::version_lock(),
 * which returns an RAII VersionLockHandle — OFD on POSIX,
 * LWE cluster-wide right on GPFS. */

const std::string mp_ns = "multipart";
const std::string MP_OBJ_PART_PFX = "part-";
const std::string MP_OBJ_HEAD_NAME = MP_OBJ_PART_PFX + "00000";
const std::string NSFS_FOLDER_OBJECT_NAME = ".folder";

/* See posix driver comment — object ownership is now read from the
 * standard RGW_ATTR_ACL attribute, matching rados. The former
 * NSFSOwner xattr could not represent account-owned objects. */

static std::string to_base36(uint64_t v)
{
  if (v == 0) return "0";
  const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  std::string result;
  while (v > 0) {
    result.insert(result.begin(), digits[v % 36]);
    v /= 36;
  }
  return result;
}

static bool from_base36(const std::string& s, uint64_t& out)
{
  out = 0;
  for (char c : s) {
    uint64_t d;
    if (c >= '0' && c <= '9') {
      d = c - '0';
    } else if (c >= 'a' && c <= 'z') {
      d = 10 + (c - 'a');
    } else {
      return false;
    }
    out = out * 36 + d;
  }
  return true;
}

static uint64_t statx_mtime_ns(const struct statx& stx)
{
  return (uint64_t)stx.stx_mtime.tv_sec * 1000000000ULL
       + stx.stx_mtime.tv_nsec;
}

static std::string nsfs_version_id_from_statx(const struct statx& stx)
{
  return "mtime-" + to_base36(statx_mtime_ns(stx))
       + "-ino-" + to_base36(stx.stx_ino);
}

struct nsfs_version_info {
  uint64_t mtime_ns{0};
  uint64_t ino{0};
};

static bool nsfs_parse_version_id(const std::string& version_id,
                                  nsfs_version_info& info)
{
  if (version_id == NULL_VERSION_ID) {
    info = {};
    return true;
  }
  static const std::string mtime_pfx = "mtime-";
  static const std::string ino_pfx = "-ino-";
  if (version_id.compare(0, mtime_pfx.size(), mtime_pfx) != 0) {
    return false;
  }
  auto ino_pos = version_id.find(ino_pfx, mtime_pfx.size());
  if (ino_pos == std::string::npos) {
    return false;
  }
  std::string mtime_str = version_id.substr(mtime_pfx.size(),
                                            ino_pos - mtime_pfx.size());
  std::string ino_str = version_id.substr(ino_pos + ino_pfx.size());
  return from_base36(mtime_str, info.mtime_ns) &&
         from_base36(ino_str, info.ino);
}

static bool is_null_version_fd(int fd)
{
  char buf[256];
  std::string vid_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
  ssize_t len = ::fgetxattr(fd, vid_xattr.c_str(), buf, sizeof(buf));
  if (len <= 0) {
    return true;
  }
  return std::string_view(buf, len) == NULL_VERSION_ID;
}

/* version entry name within .versions/: <leaf>_<version_id> */
static inline std::string nsfs_ver_entry(const std::string& leaf,
                                         const std::string& vid)
{
  return leaf + "_" + vid;
}

static std::string synthesize_etag(const struct statx& stx)
{
  return nsfs_version_id_from_statx(stx);
}


/* LMDB comparator for versioned bucket listing.
 * Key format: name \0 instance.
 * Sort: name ascending, then instance descending (newest version first).
 * Instance is "mtime-{base36}-ino-{base36}" — we extract and compare
 * the mtime numerically for correct ordering regardless of base36 width. */
static int nsfs_lmdb_cmp(const MDB_val *a, const MDB_val *b)
{
  std::string_view sa(static_cast<const char*>(a->mv_data), a->mv_size);
  std::string_view sb(static_cast<const char*>(b->mv_data), b->mv_size);

  auto sep_a = sa.find('\0');
  auto sep_b = sb.find('\0');
  std::string_view name_a = sa.substr(0, sep_a);
  std::string_view name_b = sb.substr(0, sep_b);

  int cmp = name_a.compare(name_b);
  if (cmp != 0) {
    return cmp;
  }

  /* same name — compare instances in reverse (newest first) */
  std::string_view inst_a = (sep_a != std::string_view::npos)
    ? sa.substr(sep_a + 1) : std::string_view{};
  std::string_view inst_b = (sep_b != std::string_view::npos)
    ? sb.substr(sep_b + 1) : std::string_view{};

  /* empty instance (non-versioned) sorts before any version */
  if (inst_a.empty() && inst_b.empty()) { return 0; }
  if (inst_a.empty()) { return -1; }
  if (inst_b.empty()) { return 1; }

  /* extract mtime from "mtime-{base36}-ino-{base36}" */
  auto extract_mtime = [](std::string_view inst) -> uint64_t {
    static constexpr std::string_view pfx = "mtime-";
    static constexpr std::string_view sep = "-ino-";
    if (inst.substr(0, pfx.size()) != pfx) {
      return 0;
    }
    auto ino_pos = inst.find(sep, pfx.size());
    if (ino_pos == std::string_view::npos) {
      return 0;
    }
    uint64_t val = 0;
    for (size_t i = pfx.size(); i < ino_pos; ++i) {
      char c = inst[i];
      uint64_t d;
      if (c >= '0' && c <= '9') { d = c - '0'; }
      else if (c >= 'a' && c <= 'z') { d = 10 + (c - 'a'); }
      else { return 0; }
      val = val * 36 + d;
    }
    return val;
  };

  uint64_t mt_a = extract_mtime(inst_a);
  uint64_t mt_b = extract_mtime(inst_b);

  /* descending: larger mtime sorts first */
  if (mt_a > mt_b) { return -1; }
  if (mt_a < mt_b) { return 1; }

  /* same mtime — compare full instance for determinism */
  return inst_b.compare(inst_a);
}

/*
 * Promote the newest non-delete-marker version from .versions/ to the
 * current path.  Uses CAS (safe_link) to avoid races with concurrent
 * PUT or DELETE: if a new current appears between our scan and link
 * (a concurrent writer won), the link fails with EEXIST and we skip.
 * Retries on CAS mismatch (the candidate was moved by another thread).
 */
static void promote_version(int parent_fd, const std::string& leaf,
                            const DoutPrefixProvider* dpp,
                            FSStrategy* fs_strategy)
{
  for (int attempt = 0; attempt < NSFS_VERSION_RETRIES; ++attempt) {
    /* bail if a current version already exists */
    struct statx cur_stx;
    if (statx(parent_fd, leaf.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_INO, &cur_stx) == 0) {
      ldpp_dout(dpp, 10) << "promote_version: " << leaf
        << " current already exists (ino=" << cur_stx.stx_ino
        << "), bail" << dendl;
      return;
    }

    int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                       O_RDONLY | O_DIRECTORY);
    if (vfd < 0) {
      return;
    }

    /* find newest version entry for this key */
    uint64_t max_mtime = 0;
    std::string max_name;
    DIR* vdir = fdopendir(dup(vfd));
    if (vdir) {
      struct dirent* de;
      while ((de = readdir(vdir)) != nullptr) {
        if (de->d_name[0] == '.') { continue; }
        std::string vn(de->d_name);
        if (vn.size() <= leaf.size() + 1 ||
            vn.compare(0, leaf.size(), leaf) != 0 ||
            vn[leaf.size()] != '_') {
          continue;
        }
        std::string vid = vn.substr(leaf.size() + 1);
        nsfs_version_info vi;
        if (!nsfs_parse_version_id(vid, vi)) {
          continue;
        }
        uint64_t entry_mtime;
        if (vid == NULL_VERSION_ID) {
          struct statx nstx;
          if (statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                    STATX_MTIME, &nstx) < 0) {
            continue;
          }
          entry_mtime = statx_mtime_ns(nstx);
        } else {
          entry_mtime = vi.mtime_ns;
        }
        if (entry_mtime > max_mtime || max_name.empty()) {
          max_mtime = entry_mtime;
          max_name = vn;
        }
      }
      closedir(vdir);
    }

    if (max_name.empty()) {
      ldpp_dout(dpp, 10) << "promote_version: " << leaf
        << " no versions to promote" << dendl;
      ::close(vfd);
      return;
    }
    ldpp_dout(dpp, 10) << "promote_version: " << leaf
      << " candidate=" << max_name << dendl;

    /* stat the candidate and check if it's a delete marker */
    struct statx cand_stx;
    if (statx(vfd, max_name.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_ALL, &cand_stx) < 0) {
      ::close(vfd);
      return;
    }
    {
      int chk_fd = ::openat(vfd, max_name.c_str(), O_RDONLY);
      if (chk_fd >= 0) {
        char buf[8];
        std::string dm_x = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
        if (::fgetxattr(chk_fd, dm_x.c_str(), buf, sizeof(buf)) > 0) {
          ::close(chk_fd);
          ::close(vfd);
          return;
        }
        ::close(chk_fd);
      }
    }

    /* CAS link: .versions/candidate → current path */
    SafeResult sr = fs_strategy->safe_link(dpp, vfd, max_name,
                                           parent_fd, leaf,
                                           statx_mtime_ns(cand_stx),
                                           cand_stx.stx_ino);
    ldpp_dout(dpp, 10) << "promote_version: safe_link " << max_name
      << " → " << leaf << " result=" << (int)sr << dendl;
    if (sr == SafeResult::OK) {
      /* unlink the .versions/ entry */
      ::unlinkat(vfd, max_name.c_str(), 0);
      /* remove non_current_timestamp on the promoted file */
      int pfd = ::openat(parent_fd, leaf.c_str(), O_RDONLY);
      if (pfd >= 0) {
        std::string ts_x = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_NON_CURRENT_TS;
        ::fremovexattr(pfd, ts_x.c_str());
        ::close(pfd);
      }
      ::close(vfd);
      return;
    }

    ::close(vfd);
    if (sr == SafeResult::ERROR) {
      /* EEXIST on the link means a concurrent PUT won — skip */
      return;
    }
    /* MISMATCH — candidate changed, retry */
    struct timespec ts = {0, 100000 + (rand() % 500000)};
    nanosleep(&ts, nullptr);
  }
}

static int ensure_versions_dir(int parent_fd)
{
  int ret = ::mkdirat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(), 0755);
  if (ret < 0 && errno != EEXIST) {
    return -errno;
  }
  return 0;
}

static int open_versions_dir(int parent_fd)
{
  int ret = ensure_versions_dir(parent_fd);
  if (ret < 0) {
    return ret;
  }
  int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                     O_RDONLY | O_DIRECTORY);
  if (vfd < 0) {
    return -errno;
  }
  return vfd;
}

static int open_versions_lockfile(int parent_fd)
{
  int ret = ensure_versions_dir(parent_fd);
  if (ret < 0) {
    return ret;
  }
  int fd = ::openat(parent_fd, VERSIONS_LOCKFILE.c_str(),
                    O_RDWR | O_CREAT, 0600);
  return fd < 0 ? -errno : fd;
}

namespace nsfs {

std::string get_key_fname(rgw_obj_key& key, bool use_version)
{
  std::string fname;
  if (use_version) {
    fname = key.get_oid();
  } else {
    fname = key.get_index_key_name();
  }

  if (!key.get_ns().empty()) {
    fname.insert(0, 1, '.');
  }

  if (!fname.empty() && fname.back() == '/') {
    fname += NSFS_FOLDER_OBJECT_NAME;
  }

  return fname;
}

int resolve_path(const DoutPrefixProvider* dpp,
                 Directory* root,
                 const std::string& key_path,
                 bool create_dirs,
                 CephContext* cct,
                 std::vector<std::unique_ptr<Directory>>& dir_chain,
                 Directory*& leaf_dir,
                 std::string& leaf_name)
{
  leaf_dir = root;
  leaf_name = key_path;

  size_t pos = 0;
  size_t slash;
  while ((slash = key_path.find('/', pos)) != std::string::npos) {
    std::string component = key_path.substr(pos, slash - pos);
    if (component.empty()) {
      pos = slash + 1;
      continue;
    }

    auto dir = std::make_unique<Directory>(component, leaf_dir, cct);
    int ret = dir->open(dpp);
    if (ret < 0) {
      if (!create_dirs || ret != -ENOENT) {
        return ret;
      }
      ret = dir->create(dpp);
      if (ret < 0) {
        return ret;
      }
      ret = dir->open(dpp);
      if (ret < 0) {
        return ret;
      }
    }

    leaf_dir = dir.get();
    dir_chain.push_back(std::move(dir));
    pos = slash + 1;
  }

  leaf_name = key_path.substr(pos);
  return 0;
}

} // namespace nsfs

static inline std::string gen_rand_instance_name()
{
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

#if 0
  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, OBJ_INSTANCE_LEN);
#else
  static uint64_t last_id = UINT64_MAX;
  snprintf(buf, OBJ_INSTANCE_LEN, "%lx", last_id);
  last_id--;
#endif

  return buf;
}

static inline std::string bucket_fname(std::string name, std::optional<std::string>& ns)
{
  if (ns)
    return "." + *ns + "_" + name;
  return name;
}

static int assemble_parts(const DoutPrefixProvider* dpp,
                          int dir_fd,
                          int num_parts,
                          const std::string& output_name)
{
  int out_fd = openat(dir_fd, output_name.c_str(),
                      O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
  if (out_fd < 0) {
    int ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not create assembly file "
                      << output_name << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }
  auto close_out = make_scope_guard([out_fd] { ::close(out_fd); });

  off_t out_offset = 0;
  for (int i = 1; i <= num_parts; ++i) {
    std::string part_name = MP_OBJ_PART_PFX + fmt::format("{:0>5}", i);
    int part_fd = openat(dir_fd, part_name.c_str(), O_RDONLY);
    if (part_fd < 0) {
      int ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open part " << part_name
                        << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }
    auto close_part = make_scope_guard([part_fd] { ::close(part_fd); });

    struct statx stx;
    int ret = statx(part_fd, "", AT_EMPTY_PATH, STATX_SIZE, &stx);
    if (ret < 0) {
      ret = errno;
      return -ret;
    }
    off_t remaining = stx.stx_size;
    off_t part_offset = 0;

    while (remaining > 0) {
      ssize_t copied = copy_file_range(part_fd, &part_offset,
                                       out_fd, &out_offset,
                                       remaining, 0);
      if (copied < 0) {
        if (errno == EXDEV || errno == ENOSYS || errno == EOPNOTSUPP) {
          char buf[65536];
          lseek(part_fd, part_offset, SEEK_SET);
          while (remaining > 0) {
            ssize_t nr = ::read(part_fd, buf,
                                std::min(remaining, (off_t)sizeof(buf)));
            if (nr <= 0) break;
            ssize_t nw = ::write(out_fd, buf, nr);
            if (nw != nr) return -EIO;
            remaining -= nr;
            out_offset += nr;
            part_offset += nr;
          }
          break;
        }
        ret = errno;
        ldpp_dout(dpp, 0) << "ERROR: copy_file_range failed for "
                          << part_name << ": " << cpp_strerror(ret) << dendl;
        return -ret;
      }
      remaining -= copied;
    }
  }

  return 0;
}

static inline bool get_attr(Attrs& attrs, const char* name, bufferlist& bl)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return false;
  }

  bl = iter->second;
  return true;
}

template <typename F>
static bool decode_attr(Attrs &attrs, const char *name, F &f) {
  bufferlist bl;
  if (!get_attr(attrs, name, bl)) {
    return false;
  }
  try {
    auto bufit = bl.cbegin();
    decode(f, bufit);
  } catch (buffer::error &err) {
    return false;
  }

  return true;
}


static inline rgw_obj_key decode_obj_key(const std::string& fname)
{
  rgw_obj_key key;
  rgw_obj_key::parse_raw_oid(fname, &key);
  return key;
}

static int decode_acl_owner(Attrs& attrs, ACLOwner& owner)
{
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
    return -EINVAL;
  }
  RGWAccessControlPolicy policy;
  try {
    auto bp = i->second.cbegin();
    policy.decode(bp);
  } catch (const buffer::error&) {
    return -EIO;
  }
  owner = policy.get_owner();
  return 0;
}

static inline ceph::real_time from_statx_timestamp(const struct statx_timestamp& xts)
{
  struct timespec ts{xts.tv_sec, xts.tv_nsec};
  return ceph::real_clock::from_timespec(ts);
}

static inline int copy_dir_fd(int old_fd)
{
  return openat(old_fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
}

static int get_x_attrs(optional_yield y, const DoutPrefixProvider* dpp, int fd,
		       Attrs& attrs, const std::string& display)
{
  char namebuf[64 * 1024]; // Max list size supported on linux
  ssize_t buflen;
  int ret;

  buflen = flistxattr(fd, namebuf, sizeof(namebuf));
  if (buflen < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not list attributes for " << display << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  char *keyptr = namebuf;
  while (buflen > 0) {
    std::string value;
    ssize_t vallen, keylen;
    char* vp;

    keylen = strlen(keyptr) + 1;
    std::string xattr_name(keyptr);
    std::string key;

    if (!parse_xattr_name(xattr_name, key)) {
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    vallen = fgetxattr(fd, keyptr, nullptr, 0);
    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (vallen == 0) {
      attrs.emplace(std::move(key), bufferlist{});
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    value.reserve(vallen + 1);
    vp = &value[0];

    vallen = fgetxattr(fd, keyptr, vp, vallen);
    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }

    bufferlist bl;
    bl.append(vp, vallen);
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */

    buflen -= keylen;
    keyptr += keylen;
  }

  return 0;
}

static int write_x_attr(const DoutPrefixProvider* dpp, optional_yield y, int fd,
			const std::string& key, bufferlist& value,
			const std::string& display)
{
  int ret;
  std::string attrname;

  attrname = make_xattr_name(key);

  ret = fsetxattr(fd, attrname.c_str(), value.c_str(), value.length(), 0);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not write attribute " << attrname << " for " << display << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

static int remove_x_attr(const DoutPrefixProvider *dpp, optional_yield y,
                         int fd, const std::string &key,
                         const std::string &display)
{
  int ret;
  std::string attrname{make_xattr_name(key)};

  ret = fremovexattr(fd, attrname.c_str());
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove attribute " << attrname << " for " << display << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

static int delete_directory(int parent_fd, const char* dname, bool delete_children,
		     const DoutPrefixProvider* dpp)
{
  int ret;
  int dir_fd = -1;
  DIR *dir;
  struct dirent *entry;

  dir_fd = openat(parent_fd, dname, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (dir_fd < 0) {
    dir_fd = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open subdir " << dname << ": "
                      << cpp_strerror(dir_fd) << dendl;
    return -dir_fd;
  }

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open bucket " << dname
                      << " for listing: " << cpp_strerror(ret) << dendl;
    ::close(dir_fd);
    return -ret;
  }

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    struct statx stx;

    if ((entry->d_name[0] == '.' && entry->d_name[1] == '\0') ||
        (entry->d_name[0] == '.' && entry->d_name[1] == '.' &&
         entry->d_name[2] == '\0')) {
      /* Skip . and .. */
      errno = 0;
      continue;
    }

    std::string_view d_name = entry->d_name;
    bool is_mp = d_name.starts_with("." + mp_ns);
    if (!is_mp && !delete_children) {
      closedir(dir);
      return -ENOTEMPTY;
    }

    ret = statx(dir_fd, entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name
                        << ": " << cpp_strerror(ret) << dendl;
      closedir(dir);
      return -ret;
    }

    if (S_ISDIR(stx.stx_mode)) {
      /* Recurse */
      ret = delete_directory(dir_fd, entry->d_name, true, dpp);
      if (ret < 0) {
        closedir(dir);
        return ret;
      }

      continue;
    }

    /* Otherwise, unlink */
    ret = unlinkat(dir_fd, entry->d_name, 0);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not remove file " << entry->d_name
                        << ": " << cpp_strerror(ret) << dendl;
      closedir(dir);
      return -ret;
    }
  }
  closedir(dir);

  ret = unlinkat(parent_fd, dname, AT_REMOVEDIR);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove bucket " << dname << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  return 0;
}

namespace nsfs {

FSEnt::FSEnt(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat)
  : fname(_name), parent(_parent), ctx(_ctx),
    fs_strategy(_strat ? _strat : (_parent ? _parent->fs_strategy : nullptr))
{}

FSEnt::FSEnt(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx, FSStrategy* _strat)
  : fname(_name), parent(_parent), exist(true), stx(_stx), stat_done(true), ctx(_ctx),
    fs_strategy(_strat ? _strat : (_parent ? _parent->fs_strategy : nullptr))
{}

int FSEnt::stat(const DoutPrefixProvider* dpp, bool force)
{
  if (force) {
    stat_done = false;
  }

  if (stat_done) {
    return 0;
  }

  int ret = statx(parent->get_fd(), fname.c_str(), AT_SYMLINK_NOFOLLOW,
		  STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    exist = false;
    return -ret;
  }

  exist = true;
  stat_done = true;
  return 0;
}

int FSEnt::write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs)
{
  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  need_fsync = true;

  /* Set the type */
  bufferlist type_bl;
  ObjectType type{get_type()};
  type.encode(type_bl);
  attrs[RGW_NSFS_ATTR_OBJECT_TYPE] = type_bl;

  if (fs_strategy) {
    nsfs::xattr_map_t old_raw;
    fs_strategy->get_xattrs(dpp, fd, old_raw);

    nsfs::xattr_map_t to_write;
    if (extra_attrs) {
      for (auto& [key, bl] : *extra_attrs) {
        to_write.try_emplace(make_xattr_name(key), bl.to_str());
      }
    }
    for (auto& [key, bl] : attrs) {
      to_write.try_emplace(make_xattr_name(key), bl.to_str());
    }

    std::vector<std::string> to_remove;
    for (auto& [disk_name, _] : old_raw) {
      if (disk_name.compare(0, NSFS_XATTR_PREFIX.size(),
                            NSFS_XATTR_PREFIX) != 0) {
        continue;
      }
      if (to_write.find(disk_name) == to_write.end()) {
        to_remove.push_back(disk_name);
      }
    }

    if (!to_remove.empty()) {
      ret = fs_strategy->remove_xattrs(dpp, fd, to_remove);
      if (ret < 0) {
        return ret;
      }
    }

    return fs_strategy->set_xattrs(dpp, fd, to_write);
  }

  /* per-attr syscalls */
  Attrs old_attrs;
  ret = get_x_attrs(y, dpp, fd, old_attrs, get_name());
  if (ret >= 0) {
    for (auto& it : old_attrs) {
      if (attrs.find(it.first) == attrs.end() &&
          (!extra_attrs || extra_attrs->find(it.first) == extra_attrs->end())) {
        remove_x_attr(dpp, y, fd, it.first, get_name());
      }
    }
  }

  if (extra_attrs) {
    for (auto &it : *extra_attrs) {
      ret = write_x_attr(dpp, y, fd, it.first, it.second, get_name());
      if (ret < 0) {
        return ret;
      }
    }
  }

  for (auto& it : attrs) {
    ret = write_x_attr(dpp, y, fd, it.first, it.second, get_name());
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int FSEnt::read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs)
{
  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  return get_x_attrs(y, dpp, get_fd(), attrs, get_name());
}

int FSEnt::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const std::string& path_prefix)
{
  rgw_bucket_dir_entry bde{};

  std::string full_key = path_prefix + get_name();
  rgw_obj_key key = decode_obj_key(full_key);
  if (parent->get_type() == ObjectType::MULTIPART) {
    key.ns = mp_ns;
  }
  key.get_index_key(&bde.key);
  bde.ver.pool = 1;
  bde.ver.epoch = 1;

  switch (parent->get_type().type) {
    case ObjectType::MULTIPART:
    case ObjectType::DIRECTORY:
      bde.exists = true;
      break;
    case ObjectType::UNKNOWN:
    case ObjectType::FILE:
      return -EINVAL;
  }

  Attrs attrs;
  int ret = open(dpp);
  if (ret < 0)
    return ret;

  ret = get_x_attrs(y, dpp, get_fd(), attrs, get_name());
  if (ret < 0)
    return ret;

  ACLOwner acl_owner;
  ret = decode_acl_owner(attrs, acl_owner);
  if (ret < 0) {
    bde.meta.owner = "unknown";
    bde.meta.owner_display_name = "unknown";
  } else {
    bde.meta.owner = to_string(acl_owner.id);
    bde.meta.owner_display_name = acl_owner.display_name;
  }
  bde.meta.category = RGWObjCategory::Main;
  bde.meta.size = stx.stx_size;
  bde.meta.accounted_size = stx.stx_size;
  bde.meta.mtime = from_statx_timestamp(stx.stx_mtime);
  bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
  bde.meta.appendable = true;
  bufferlist etag_bl;
  if (rgw::sal::get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
    bde.meta.etag = etag_bl.to_str();
  } else {
    bde.meta.etag = synthesize_etag(stx);
  }

  if (flags & FLAG_LIST_VERSIONS) {
    std::string ver_id;
    if (is_null_version_fd(get_fd())) {
      ver_id = NULL_VERSION_ID;
    } else {
      ver_id = nsfs_version_id_from_statx(stx);
    }
    bde.key.instance = ver_id;
    bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                rgw_bucket_dir_entry::FLAG_CURRENT;
    std::string dm_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
    char dm_buf[8];
    ssize_t dm_len = ::fgetxattr(get_fd(), dm_xattr.c_str(),
                                 dm_buf, sizeof(dm_buf));
    if (dm_len > 0) {
      bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
    }
  }

  return cb(dpp, bde);
}

int File::create(const DoutPrefixProvider *dpp, bool* existed, bool temp_file)
{
  int flags, ret;
  std::string path;
  if(temp_file) {
    flags = O_TMPFILE | O_RDWR;
    path = ".";
  } else {
    flags = O_CREAT | O_RDWR;
    path = get_name();
  }

  ret = openat(parent->get_fd(), path.c_str(), flags | O_NOFOLLOW, S_IRWXU);
  if (ret < 0) {
    ret = errno;
    if (ret == EEXIST) {
      return 0;
    }
    ldpp_dout(dpp, 0) << "ERROR: could not open object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  fd = ret;
  need_fsync = true;

  return 0;
}

int File::open(const DoutPrefixProvider* dpp)
{
  if (fd >= 0) {
    return 0;
  }

  int ret = openat(parent->get_fd(), fname.c_str(), O_RDWR, S_IRWXU);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  fd = ret;

  return 0;
}

int File::close()
{
  if (fd < 0) {
    return 0;
  }

  if (need_fsync) {
    int ret = ::fsync(fd);
    if (ret < 0) {
      return ret;
    }
    need_fsync = false;
  }

  int ret = ::close(fd);
  if(ret < 0) {
    return ret;
  }
  fd = -1;

  return 0;
}


int File::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = FSEnt::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  if (!S_ISREG(stx.stx_mode)) {
    /* Not a file */
    ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " is not a file" << dendl;
    return -EINVAL;
  }

  return 0;
}

int File::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  need_fsync = true;
  int64_t left = bl.length();
  char* curp = bl.c_str();
  ssize_t ret;

  ret = fchmod(fd, S_IRUSR|S_IWUSR);
  if(ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not change permissions on object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return ret;
  }


  ret = lseek(fd, ofs, SEEK_SET);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
      << ofs << " :" << cpp_strerror(ret) << dendl;
    return -ret;
  }

  while (left > 0) {
    ret = ::write(fd, curp, left);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not write object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    curp += ret;
    left -= ret;
  }

  return 0;
}

int File::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t len = std::min(left, READ_SIZE);
  ssize_t ret;

  ret = lseek(fd, ofs, SEEK_SET);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
                      << ofs << " :" << cpp_strerror(ret) << dendl;
    return -ret;
    }

    char read_buf[READ_SIZE];
    ret = ::read(fd, read_buf, len);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    bl.append(read_buf, ret);

    return ret;
}

int File::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name)
{
  /* remove any existing target */
  {
    std::unique_ptr<FSEnt> del;
    int ret = dst_dir->get_ent(dpp, y, dst_name, std::string(), del);
    if (ret >= 0) {
      ret = del->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << dst_name
                          << dendl;
        return ret;
      }
    }
  }

  int src_dir_fd = parent->get_fd();
  if (src_dir_fd < 0) {
    parent->open(dpp);
    src_dir_fd = parent->get_fd();
  }
  int dst_dir_fd = dst_dir->get_fd();
  if (dst_dir_fd < 0) {
    dst_dir->open(dpp);
    dst_dir_fd = dst_dir->get_fd();
  }

  return fs_strategy->clone_file(dpp, src_dir_fd, get_name(),
                                 dst_dir_fd, dst_name);
}

int File::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  if (!exists()) {
    return 0;
  }

  int parent_fd = parent->get_fd();
  int ret = unlinkat(parent_fd, fname.c_str(), 0);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name()
                        << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  if (fs_strategy) {
    fs_strategy->cleanup_clone(dpp, parent_fd, fname);
  }

  return 0;
}

int File::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y, std::string temp_fname)
{
  if (fd < 0) {
    return 0;
  }

  int ret = fs_strategy->link_temp_file(fd, parent->get_fd(),
                                        get_name(), dpp);
  if (ret < 0) {
    return ret;
  }

  /* note that open() and stat() return already sign-reversed result codes */
  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed closing file" << dendl;
    return ret;
  }

  return 0;
}

bool Directory::file_exists(std::string& name)
{
  struct statx nstx;
  int ret = statx(fd, name.c_str(), AT_SYMLINK_NOFOLLOW, STATX_ALL, &nstx);

  return (ret >= 0);
}

int Directory::create(const DoutPrefixProvider* dpp, bool* existed, bool temp_file)
{
  if (temp_file) {
    ldpp_dout(dpp, 0) << "ERROR: cannot create directory with temp_file " << get_name() << dendl;
    return -EINVAL;
  }

  int ret = mkdirat(parent->get_fd(), fname.c_str(), S_IRWXU);
  if (ret < 0) {
    ret = errno;
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }

  return 0;
}

int Directory::open(const DoutPrefixProvider* dpp)
{
  if (fd >= 0) {
    return 0;
  }

  int pfd{AT_FDCWD};
  if (parent)
    pfd = parent->get_fd();

  int ret = openat(pfd, fname.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  fd = ret;

  return 0;
}

int Directory::close()
{
  if (fd < 0) {
    return 0;
  }

  ::close(fd);
  fd = -1;

  return 0;
}

int Directory::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = FSEnt::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  if (!S_ISDIR(stx.stx_mode)) {
    /* Not a directory */
    ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " is not a directory" << dendl;
    return -EINVAL;
  }

  return 0;
}

int Directory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  return delete_directory(parent->get_fd(), fname.c_str(), delete_children, dpp);
}

int Directory::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		     optional_yield y)
{
  return -EINVAL;
}

int Directory::read(int64_t ofs, int64_t left, bufferlist &bl,
                    const DoutPrefixProvider *dpp, optional_yield y)
{
  return -EINVAL;
}

int Directory::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
                              std::string temp_fname)
{
  return -EINVAL;
}

template <typename F>
int Directory::for_each(const DoutPrefixProvider* dpp, const F& func)
{
  DIR* dir;
  struct dirent* entry;
  int ret;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  dir = fdopendir(fd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name() << " for listing: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  rewinddir(dir);

  ret = 0;
  while ((entry = readdir(dir)) != NULL) {
    std::string_view vname(entry->d_name);

    if (vname == "." || vname == "..")
      continue;

    int r = func(entry->d_name);
    if (r < 0) {
      ret = r;
      break;
    }
  }

  if (ret == -EAGAIN) {
    /* Limit reached */
    ret = 0;
  }

  closedir(dir);
  // closedir() closes the fd, so we need to invalidate it
  fd = -1;
  // closedir() closes fd, but succeeding calls might assume that fd is still valid.
  // so let's reopen it.
  open(dpp);
  return ret;
}

int Directory::rename(const DoutPrefixProvider* dpp, optional_yield y, Directory* dst_dir, std::string dst_name)
{
  int flags = 0;
  int ret;
  std::string src_name = fname;
  int parent_fd = parent->get_fd();

  if (dst_dir->file_exists(dst_name)) {
    flags = RENAME_EXCHANGE;
  }
  // swap
  ret = renameat2(parent_fd, src_name.c_str(), dst_dir->get_fd(), dst_name.c_str(), flags);
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: renameat2 for shadow object could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }

  /* Parent of this dir is now dest dir */
  parent = dst_dir;
  /* Name has changed */
  fname = dst_name;

  // Delete old one (could be file or directory)
  struct statx stx;
  ret = statx(parent_fd, src_name.c_str(), AT_SYMLINK_NOFOLLOW,
		  STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    if (ret == ENOENT) {
      return 0;
    }
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  if (S_ISREG(stx.stx_mode)) {
    ret = unlinkat(parent_fd, src_name.c_str(), 0);
  } else if (S_ISDIR(stx.stx_mode)) {
    ret = delete_directory(parent_fd, src_name.c_str(), true, dpp);
  }
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove old file " << get_name()
                      << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int Directory::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name)
{
  int ret;

  // Delete the target
  {
    std::unique_ptr<FSEnt> del;
    ret = dst_dir->get_ent(dpp, y, dst_name, std::string(), del);
    if (ret >= 0) {
      ret = del->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << dst_name
                          << dendl;
        return ret;
      }
    }
  }

  ret = dst_dir->open(dpp);
  std::unique_ptr<Directory> dest = clone_dir();
  dest->parent = dst_dir;
  dest->fname = dst_name;

  ret = dest->create(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not create dest " << dest->get_name() << dendl;
    return ret;
  }

  Attrs attrs;
  ret = read_attrs(dpp, y, attrs);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not read attrs from " << get_name() << dendl;
    return ret;
  }
  ret = dest->write_attrs(dpp, y, attrs, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not write attrs to " << dest->get_name() << dendl;
    return ret;
  }

  ret = for_each(dpp, [this, &dest, &dpp, &y](const char* name) {
    std::unique_ptr<FSEnt> sobj;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    int r = this->get_ent(dpp, y, name, std::string(), sobj);
    if (r < 0)
      return r;
    return sobj->copy(dpp, y, dest.get(), name);
  });

  return ret;
}

int Directory::get_ent(const DoutPrefixProvider *dpp, optional_yield y, const std::string &name, const std::string& instance, std::unique_ptr<FSEnt>& ent)
{
  struct statx nstx;
  std::unique_ptr<FSEnt> nent;

  int ret = open(dpp);
  if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open directory " << name << dendl;
      return ret;
  }

  ret = statx(get_fd(), name.c_str(),
                  AT_SYMLINK_NOFOLLOW, STATX_ALL, &nstx);
  if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << " in dir "
                        << get_name() << " : " << cpp_strerror(ret) << dendl;
      return -ret;
  }
  if (S_ISREG(nstx.stx_mode)) {
    nent = std::make_unique<File>(name, this, nstx, ctx);
  } else if (S_ISDIR(nstx.stx_mode)) {
    ObjectType type{ObjectType::DIRECTORY};
    int tmpfd;
    Attrs attrs;

    tmpfd = openat(get_fd(), name.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (tmpfd >= 0) {
      ret = get_x_attrs(y, dpp, tmpfd, attrs, name);
      if (ret >= 0) {
        decode_attr(attrs, RGW_NSFS_ATTR_OBJECT_TYPE, type);
      }
      ::close(tmpfd);
    }
    switch (type.type) {
    case ObjectType::MULTIPART:
      nent = std::make_unique<MPDirectory>(name, this, nstx, ctx);
      break;
    case ObjectType::DIRECTORY:
      nent = std::make_unique<Directory>(name, this, nstx, ctx);
      break;
    default:
      ldpp_dout(dpp, 0) << "ERROR: invalid type " << type << dendl;
      return -EINVAL;
    }
  } else {
    return -EINVAL;
  }

  ent.swap(nent);
  return 0;
}

int Directory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                          fill_cache_cb_t &cb, uint32_t flags,
                          const std::string& path_prefix)
{
  int ret = for_each(dpp, [this, &cb, &dpp, &y, &path_prefix, flags](const char *name) {
    std::unique_ptr<FSEnt> ent;

    if (name[0] == '.' && name != NSFS_FOLDER_OBJECT_NAME) {
      return 0;
    }

    int ret = get_ent(dpp, y, name, std::string(), ent);
    if (ret < 0)
      return ret;

    ent->stat(dpp);

    if (name == NSFS_FOLDER_OBJECT_NAME) {
      // directory object sentinel: emit with key = path_prefix
      // path_prefix is already "photos/" when inside photos/
      rgw_bucket_dir_entry bde{};
      rgw_obj_key key = decode_obj_key(path_prefix);
      key.get_index_key(&bde.key);
      bde.ver.pool = 1;
      bde.ver.epoch = 1;
      bde.exists = true;

      ret = ent->open(dpp);
      if (ret < 0)
        return ret;

      Attrs attrs;
      ret = get_x_attrs(y, dpp, ent->get_fd(), attrs, ent->get_name());
      if (ret < 0)
        return ret;

      ACLOwner acl_owner;
      if (decode_acl_owner(attrs, acl_owner) >= 0) {
        bde.meta.owner = to_string(acl_owner.id);
        bde.meta.owner_display_name = acl_owner.display_name;
      } else {
        bde.meta.owner = "unknown";
        bde.meta.owner_display_name = "unknown";
      }
      bde.meta.category = RGWObjCategory::Main;
      bde.meta.size = ent->get_stx().stx_size;
      bde.meta.accounted_size = ent->get_stx().stx_size;
      bde.meta.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);
      bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;

      bufferlist etag_bl;
      if (rgw::sal::get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
        bde.meta.etag = etag_bl.to_str();
      } else {
        bde.meta.etag = synthesize_etag(ent->get_stx());
      }

      return cb(dpp, bde);
    }

    if (ent->get_type() == ObjectType::DIRECTORY) {
      Directory* subdir = static_cast<Directory*>(ent.get());
      ret = subdir->open(dpp);
      if (ret < 0)
        return ret;
      return subdir->fill_cache(dpp, y, cb, flags,
                                path_prefix + name + "/");
    }

    ret = ent->fill_cache(dpp, y, cb, flags, path_prefix);
    if (ret < 0)
      return ret;
    return 0;
  });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list directory " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return ret;
  }

  /* enumerate .versions/ for versioned buckets */
  if (flags & FSEnt::FLAG_LIST_VERSIONS) {
    int vfd = ::openat(get_fd(), HIDDEN_VERSIONS_PATH.c_str(),
                       O_RDONLY | O_DIRECTORY);
    if (vfd >= 0) {
      DIR* vdir = fdopendir(vfd);
      if (vdir) {
        struct dirent* de;
        while ((de = readdir(vdir)) != nullptr) {
          if (de->d_name[0] == '.') {
            continue;
          }

          std::string vname(de->d_name);
          /* parse "key_version_id" — find the last '_mtime-' or '_null' */
          std::string obj_name;
          std::string ver_id;
          auto null_pos = vname.rfind("_null");
          if (null_pos != std::string::npos &&
              null_pos + 5 == vname.size()) {
            obj_name = vname.substr(0, null_pos);
            ver_id = NULL_VERSION_ID;
          } else {
            auto mtime_pos = vname.rfind("_mtime-");
            if (mtime_pos != std::string::npos) {
              obj_name = vname.substr(0, mtime_pos);
              ver_id = vname.substr(mtime_pos + 1);
            } else {
              continue;
            }
          }

          struct statx vstx;
          if (statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                    STATX_ALL, &vstx) < 0) {
            continue;
          }

          rgw_bucket_dir_entry bde{};
          std::string full_key = path_prefix + obj_name;
          bde.key.name = full_key;
          bde.key.instance = ver_id;
          bde.ver.pool = 1;
          bde.ver.epoch = 1;
          bde.exists = true;
          bde.flags = rgw_bucket_dir_entry::FLAG_VER;

          /* check delete marker */
          int tfd = ::openat(vfd, de->d_name, O_RDONLY);
          if (tfd >= 0) {
            char buf[8];
            std::string dm_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
            ssize_t xlen = ::fgetxattr(tfd, dm_xattr.c_str(), buf, sizeof(buf));
            if (xlen > 0) {
              bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
              /* a delete marker in .versions/ is current if no file
               * exists at the top-level path for this key */
              if (::faccessat(get_fd(), obj_name.c_str(),
                              F_OK, AT_SYMLINK_NOFOLLOW) != 0) {
                bde.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
              }
            }

            Attrs attrs;
            ret = get_x_attrs(y, dpp, tfd, attrs, vname);

            ACLOwner acl_owner;
            if (decode_acl_owner(attrs, acl_owner) >= 0) {
              bde.meta.owner = to_string(acl_owner.id);
              bde.meta.owner_display_name = acl_owner.display_name;
            }

            bufferlist etag_bl;
            if (rgw::sal::get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
              bde.meta.etag = etag_bl.to_str();
            }

            ::close(tfd);
          }

          bde.meta.category = RGWObjCategory::Main;
          bde.meta.size = vstx.stx_size;
          bde.meta.accounted_size = vstx.stx_size;
          bde.meta.mtime = from_statx_timestamp(vstx.stx_mtime);
          bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;

          if (bde.meta.etag.empty()) {
            bde.meta.etag = synthesize_etag(vstx);
          }

          ret = cb(dpp, bde);
          if (ret < 0) {
            break;
          }
        }
        closedir(vdir);
      } else {
        ::close(vfd);
      }
    }
  }

  return 0;
}

int MPDirectory::create(const DoutPrefixProvider* dpp, bool* existed, bool temp_file)
{
  std::string path;

  if(temp_file) {
    tmpname = path = "._tmpname_" +
           std::to_string(ceph::util::generate_random_number<uint64_t>());
  } else {
    path = get_name();
  }

  int ret = mkdirat(parent->get_fd(), path.c_str(), S_IRWXU);
  if (ret < 0) {
    ret = errno;
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }

  return 0;
}

int MPDirectory::read(int64_t ofs, int64_t left, bufferlist &bl,
                    const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string pname;
  for (auto part : parts) {
    if (ofs < part.second) {
      pname = part.first;
      break;
    }

    ofs -= part.second;
  }

  if (pname.empty()) {
    // ofs is past the end
    return 0;
  }

  if (!cur_read_part || cur_read_part->get_name() != pname) {
    cur_read_part = std::make_unique<File>(pname, this, ctx);
  }
  int ret = cur_read_part->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return cur_read_part->read(ofs, left, bl, dpp, y);
}

int MPDirectory::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
                                std::string temp_fname)
{
  if (tmpname.empty()) {
    return 0;
  }

  /* Temporarily change name to tmpname, so we can reuse rename() */
  std::string savename = fname;
  fname = tmpname;
  tmpname.clear();

  return rename(dpp, y, parent, savename);
}

int MPDirectory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  return Directory::remove(dpp, y, /*delete_children=*/true);
}

int MPDirectory::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = Directory::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  uint64_t total_size{0};
  for_each(dpp, [this, &total_size, &dpp](const char *name) {
    int ret;
    struct statx stx;
    std::string sname = name;

    if (sname.rfind(MP_OBJ_PART_PFX, 0) != 0) {
      /* Skip non-parts */
      return 0;
    }

    ret = statx(fd, name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
                        << cpp_strerror(ret) << dendl;
      return -ret;
    }

    if (!S_ISREG(stx.stx_mode)) {
      /* Skip non-files */
      return 0;
    }

    parts[name] = stx.stx_size;
    total_size += stx.stx_size;
    return 0;
  });

  stx.stx_size = total_size;

  return 0;
}


std::unique_ptr<File> MPDirectory::get_part_file(int partnum)
{
  std::string partname = MP_OBJ_PART_PFX + fmt::format("{:0>5}", partnum);
  rgw_obj_key part_key(partname);

  return std::make_unique<File>(partname, this, ctx);
}

int MPDirectory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                            fill_cache_cb_t &cb, uint32_t flags,
                            const std::string& path_prefix)
{
  int ret = FSEnt::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE, path_prefix);
  if (ret < 0)
    return ret;

  return Directory::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE, path_prefix);
}

} // namespace nsfs


bool NSFSZoneGroup::placement_target_exists(std::string& target) const {
  return !!group->placement_targets.count(target);
}

void NSFSZoneGroup::get_placement_target_names(std::set<std::string>& names) const {
  for (const auto& target : group->placement_targets) {
    names.emplace(target.second.name);
  }
}

ZoneGroup& NSFSZone::get_zonegroup() {
  return *zonegroup;
}

const RGWZoneParams& NSFSZone::get_rgw_params() {
  return *zone_params;
}

const std::string& NSFSZone::get_id() {
  return zone_params->get_id();
}

const std::string& NSFSZone::get_name() const {
  return zone_params->get_name();
}

bool NSFSZone::is_writeable() {
  return true;
}

bool NSFSZone::get_redirect_endpoint(std::string* endpoint) {
  return false;
}

const std::string& NSFSZone::get_current_period_id() {
  return current_period->get_id();
}

const RGWAccessKey& NSFSZone::get_system_key() {
  return zone_params->system_key;
}

const std::string& NSFSZone::get_realm_name() {
  return realm->get_name();
}

const std::string& NSFSZone::get_realm_id() {
  return realm->get_id();
}

RGWBucketSyncPolicyHandlerRef NSFSZone::get_sync_policy_handler() {
  return nullptr;
}

int NSFSLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
{
  return -ENOENT;
}

std::tuple<rgw::lua::LuaCodeType, int> NSFSLuaManager::get_script_or_bytecode(const DoutPrefixProvider* dpp, optional_yield y,
                                                                               const std::string& key)
{
  return std::make_tuple("", -ENOENT);
}

int NSFSLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
{
  return -ENOENT;
}

int NSFSLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
{
  return -ENOENT;
}

int NSFSLuaManager::add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int NSFSLuaManager::remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int NSFSLuaManager::list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages)
{
  return -ENOENT;
}

int NSFSLuaManager::reload_packages(const DoutPrefixProvider* dpp, optional_yield y)
{
  return -ENOENT;
}

int NSFSDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  int ret = -1;
  base_path = g_conf().get_val<std::string>("rgw_nsfs_base_path");

  ldpp_dout(dpp, 20) << "Initializing NSFS driver: " << base_path << dendl;

  auto gpfs_lib = g_conf().get_val<std::string>("rgw_nsfs_gpfs_lib_path");
  bool gpfs_clone = g_conf().get_val<bool>("rgw_nsfs_gpfs_clone_files");
  bool gpfs_lwe = g_conf().get_val<bool>("rgw_nsfs_gpfs_lwe_locking");
  bool gpfs_batch = g_conf().get_val<bool>("rgw_nsfs_gpfs_batch_xattrs");
  fs_strategy = nsfs::GPFSStrategy::try_create(dpp, gpfs_lib, base_path,
                                               gpfs_clone, gpfs_lwe,
                                               gpfs_batch);
  if (!fs_strategy) {
    fs_strategy = std::make_unique<nsfs::POSIXStrategy>();
  }
  ldpp_dout(dpp, 1) << "nsfs: using " << fs_strategy->name()
    << " fs strategy" << dendl;

  /* ordered listing cache */
  bucket_cache.reset(
    new BucketCache(
      this, base_path,
      g_conf().get_val<std::string>("rgw_nsfs_database_root"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_max_buckets"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_lanes"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_partitions"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_lmdb_count"),
      g_conf().get_val<bool>("rgw_nsfs_inotify")));

  /* user info cache */
  user_cache.set_max_size(dpp, g_conf().get_val<uint64_t>("rgw_nsfs_cache_max_users"));

  root_dir = std::make_unique<Directory>(base_path, nullptr, ctx(), fs_strategy.get());
  ret = root_dir->open(dpp);
  if (ret < 0) {
    if (ret == -ENOTDIR) {
      ldpp_dout(dpp, 0) << " ERROR: base path (" << base_path
	<< "): was not a directory." << dendl;
      return ret;
    } else if (ret == -ENOENT) {
      ret = root_dir->create(dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not create base path ("
	  << base_path << "): " << cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
  }
  lc = new RGWLC();
  lc->initialize(cct, this);

  if (use_lc_thread) {
    ret = userDB->createLCTables(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Failed to create LC tables, ret=" << ret << dendl;
      return ret;
    }
    lc->start_processor();
  }

  ldpp_dout(dpp, 20) << "root_fd: " << root_dir->get_fd() << dendl;
  quota_handler = RGWQuotaHandler::generate_handler(dpp, this, true);

  if (!RGWPubSubEndpoint::init_all(cct)) {
    ldpp_dout(dpp, 1) << "WARNING: failed to init notification endpoints" << dendl;
  }

  ldpp_dout(dpp, 20) << "SUCCESS" << dendl;
  return 0;
}

void NSFSDriver::finalize()
{
  RGWPubSubEndpoint::shutdown_all();
  RGWQuotaHandler::free_handler(quota_handler);
}

std::unique_ptr<User> NSFSDriver::get_user(const rgw_user &u)
{
  return std::make_unique<NSFSUser>(this, u);
}

int NSFSDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  {
    UserCacheEntry ce;
    if (user_cache.lookup_user_by_access_key(dpp, key, ce)) {
      auto u = new NSFSUser(this, ce.info);
      u->get_attrs() = ce.attrs;
      u->get_version_tracker() = ce.objv_tracker;
      user->reset(u);
      return 0;
    }
  }

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("access_key"), key, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new NSFSUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int NSFSDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("email"), email, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new NSFSUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int NSFSDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  /* Swift keys and subusers are not supported by DBStore for now */
  return -ENOTSUP;
}

int NSFSDriver::load_account_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view id,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("account_id"), std::string(id), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::load_account_by_name(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view name,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("name"), std::string(name), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::load_account_by_email(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view email,
				  RGWAccountInfo& info,
				  Attrs& attrs,
				  RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("email"), std::string(email), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::store_account(const DoutPrefixProvider* dpp,
			  optional_yield y, bool exclusive,
			  const RGWAccountInfo& info,
			  const RGWAccountInfo* old_info,
			  const Attrs& attrs,
			  RGWObjVersionTracker& objv)
{
  int ret = userDB->store_account(dpp, info, exclusive, &attrs, &objv);

  if (ret < 0)
    return ret;

  return 0;
}

int NSFSDriver::delete_account(const DoutPrefixProvider* dpp,
			     optional_yield y,
			     const RGWAccountInfo& info,
			     RGWObjVersionTracker& objv)
{
  int ret = userDB->remove_account(dpp, info, &objv);

  if (ret < 0)
    return ret;

  return 0;
}



int NSFSDriver::load_owner_by_email(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view email,
				    rgw_owner& owner)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_user(dpp, "email", std::string{email},
				   uinfo, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }
  owner = std::move(uinfo.user_id);
  return 0;
}

std::unique_ptr<Object> NSFSDriver::get_object(const rgw_obj_key& k)
{
  return std::make_unique<NSFSObject>(this, k);
}

int NSFSDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  *bucket = std::make_unique<NSFSBucket>(this, root_dir.get(), b);
  return (*bucket)->load_bucket(dpp, y);
}

std::unique_ptr<Bucket> NSFSDriver::get_bucket(const RGWBucketInfo& i)
{
  /* Don't need to fetch the bucket info, use the provided one */
  return std::make_unique<NSFSBucket>(this, root_dir.get(), i);
}

std::string NSFSDriver::zone_unique_trans_id(const uint64_t unique_num)
{
  char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
  time_t timestamp = time(NULL);

  snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
           (unsigned long long)unique_num,
           (unsigned long long)timestamp);

  return std::string(buf);
}

int NSFSDriver::get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zg)
{
  /* XXX: for now only one zonegroup supported */
  std::unique_ptr<RGWZoneGroup> rzg =
      std::make_unique<RGWZoneGroup>("default", "default");
  rzg->api_name = "default";
  rzg->is_master = true;
  ZoneGroup* group = new NSFSZoneGroup(this, std::move(rzg));
  if (!group)
    return -ENOMEM;

  zg->reset(group);
  return 0;
}

int NSFSDriver::list_all_zones(const DoutPrefixProvider* dpp,
			    std::list<std::string>& zone_ids)
{
  zone_ids.push_back(zone.get_id());
  return 0;
}

int NSFSDriver::cluster_stat(RGWClusterStat& stats)
{
  return 0;
}

std::unique_ptr<Lifecycle> NSFSDriver::get_lifecycle(void)
{
  return std::make_unique<NSFSLifecycle>(this);
}

std::unique_ptr<Writer> NSFSDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  return nullptr;
}

std::unique_ptr<Writer> NSFSDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{

  return std::make_unique<NSFSAtomicWriter>(dpp, y, _head_obj, this, owner, ptail_placement_rule, olh_epoch, unique_tag);
}

const std::string& NSFSDriver::get_compression_type(const rgw_placement_rule& rule) {
  return zone.get_rgw_params().get_compression_type(rule);
}

std::unique_ptr<Notification> NSFSDriver::get_notification(rgw::sal::Object* obj,
			      rgw::sal::Object* src_obj, struct req_state* s,
			      rgw::notify::EventType event_type, optional_yield y,
			      const std::string* object_name)
{
  rgw::notify::EventTypeList event_types = {event_type};
  auto notif = std::make_unique<NSFSNotification>(this, obj, src_obj, event_types,
      s->bucket.get(),
      to_string(s->owner.id),
      s->owner.id.index() == 0 ? std::get<rgw_user>(s->owner.id).tenant : "",
      s->req_id);
  notif->x_meta_map = s->info.x_meta_map;
  return notif;
}

std::unique_ptr<Notification> NSFSDriver::get_notification(
    const DoutPrefixProvider* dpp,
    rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    const rgw::notify::EventTypeList& event_types,
    rgw::sal::Bucket* _bucket,
    std::string& _user_id,
    std::string& _user_tenant,
    std::string& _req_id,
    optional_yield y) {
  return std::make_unique<NSFSNotification>(this, obj, src_obj, event_types,
      _bucket, _user_id, _user_tenant, _req_id);
}

// TODO: marker and other params
int NSFSDriver::list_buckets(const DoutPrefixProvider* dpp, const rgw_owner& owner,
			     const std::string& tenant, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &result, optional_yield y)
{
  DIR* dir;
  struct dirent* entry;
  int dfd;
  int ret;

  result.buckets.clear();

  /* it's not sufficient to dup(root_fd), as as the new fd would share
   * the file position of root_fd */
  dfd = copy_dir_fd(get_root_fd());
  if (dfd == -1) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    return -errno;
  }

  dir = fdopendir(dfd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
      << cpp_strerror(ret) << dendl;
    ::close(dfd);
    return -ret;
  }

  auto cleanup_guard = make_scope_guard(
    [&dir]
      {
	closedir(dir);
	// dfd is also closed
      }
    );

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    struct statx stx;

    ret = statx(get_root_fd(), entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    if (!S_ISDIR(stx.stx_mode)) {
      /* Not a bucket, skip it */
      errno = 0;
      continue;
    }
    if (entry->d_name[0] == '.') {
      /* Skip dotfiles */
      errno = 0;
      continue;
    }
    std::unique_ptr<Bucket> bucket;
    ret = load_bucket(dpp, rgw_bucket("", entry->d_name), &bucket, null_yield);
    if (bucket->get_owner() != owner) {
      continue;
    }
    RGWBucketEnt ent;
    ent.bucket.name = entry->d_name;
    ent.creation_time = ceph::real_clock::from_time_t(stx.stx_btime.tv_sec);
    // TODO: ent.size and ent.count

    result.buckets.push_back(std::move(ent));
    errno = 0;
    if (result.buckets.size() == max){
      result.next_marker = ent.bucket.marker;
      break;
    }
  }
  ret = errno;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list buckets for " << owner << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int NSFSBucket::create(const DoutPrefixProvider* dpp,
			const CreateParams& params,
			optional_yield y)
{
  info.owner = params.owner;

  if (params.marker.empty()) {
    char buf[17];
    gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
    buf[16] = '\0';
    info.bucket.marker = info.bucket.name + "." + buf;
    info.bucket.bucket_id = info.bucket.marker;
  } else {
    info.bucket.marker = params.marker;
    info.bucket.bucket_id = params.bucket_id;
  }

  info.zonegroup = params.zonegroup_id;
  info.placement_rule = params.placement_rule;
  info.swift_versioning = params.swift_ver_location.has_value();
  if (params.swift_ver_location) {
    info.swift_ver_location = *params.swift_ver_location;
  }
  if (params.obj_lock_enabled) {
    info.flags |= BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
  }
  info.requester_pays = false;
  if (params.creation_time) {
    info.creation_time = *params.creation_time;
  } else {
    info.creation_time = ceph::real_clock::now();
  }
  if (params.quota) {
    info.quota = *params.quota;
  }

  int ret = set_attrs(params.attrs);
  if (ret < 0) {
    return ret;
  }

  bool existed = false;
  ret = create(dpp, y, &existed);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int NSFSUser::load_user_from_cache_or_db(const DoutPrefixProvider* dpp, bool& cache_hit)
{
  cache_hit = false;
  UserCacheEntry ce;
  if (driver->get_user_cache().lookup_user_by_uid(dpp, this->get_id().id, ce)) {
    this->get_info() = std::move(ce.info);
    this->get_attrs() = std::move(ce.attrs);
    this->get_version_tracker() = std::move(ce.objv_tracker);
    cache_hit = true;
    return 0;
  }

  int ret = driver->get_user_db()->get_user(dpp, std::string("user_id"), this->get_id().id, this->get_info(), &(this->get_attrs()),
        &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  return ret;
}

int NSFSUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: read_attrs: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int NSFSUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  auto attrs = this->get_attrs();
  for(auto& it : new_attrs) {
	attrs[it.first] = it.second;
  }
  this->get_attrs() = std::move(attrs);

  return store_user(dpp, y, false);
}

int NSFSUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: load_user: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int NSFSUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  int ret = driver->get_user_db()->store_user(dpp, this->get_info(), exclusive, &(this->get_attrs()), &(this->get_version_tracker()), old_info);
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  return ret;
}

int NSFSUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = driver->get_user_db()->remove_user(dpp, this->get_info(), &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to remove user uid=" << this->get_id().id << " ret=" << ret << dendl;
  }
  return ret;
}

int NSFSUser::list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                          std::string_view marker, uint32_t max_items,
                          GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = driver->get_user_db()->list_user_groups(dpp,
      get_id().id, std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int NSFSUser::verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider *dpp, optional_yield y)
{
  *verified = false;
  return 0;
}

std::unique_ptr<Object> NSFSBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<NSFSObject>(driver, k, this);
}

int NSFSObject::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  return ent->fill_cache(dpp, y, cb, FSEnt::FLAG_NONE);
}

int NSFSDriver::mint_listing_entry(const std::string &bname,
                                    rgw_bucket_dir_entry &bde) {
    std::unique_ptr<Bucket> b;
    std::unique_ptr<Object> obj;
    NSFSObject *pobj;
    int ret;

    ret = load_bucket(nullptr, rgw_bucket(std::string(), bname),
                      &b, null_yield);
    if (ret < 0)
      return ret;

    obj = b->get_object(decode_obj_key(bde.key.name));
    pobj = static_cast<NSFSObject *>(obj.get());

    if (!pobj->check_exists(nullptr)) {
      ret = errno;
      return -ret;
    }

    ret = pobj->get_obj_attrs(null_yield, nullptr);
    if (ret < 0)
      return ret;

    ret = pobj->fill_cache(nullptr, null_yield,
        [&bde](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &nbde) -> int {
	  bde = nbde;
	  return 0;
        });

    return ret;
}

std::unique_ptr<LuaManager> NSFSDriver::get_lua_manager(const std::string& luarocks_path)
{
  return std::make_unique<NSFSLuaManager>(this);
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(std::string name,
    std::string tenant,
    rgw_account_id account_id,
    std::string path,
    std::string trust_policy,
    std::string description,
    std::string max_session_duration_str,
    std::multimap<std::string,std::string> tags)
{
  return std::make_unique<DBStoreRole>(
      get_user_db(), std::move(name), std::move(tenant),
      std::move(account_id), std::move(path), std::move(trust_policy),
      std::move(description), std::move(max_session_duration_str),
      std::move(tags));
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(std::string id)
{
  return std::make_unique<DBStoreRole>(get_user_db(), std::move(id));
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(const RGWRoleInfo& info)
{
  return std::make_unique<DBStoreRole>(get_user_db(), info);
}

int NSFSDriver::count_account_roles(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    uint32_t& count)
{
  return get_user_db()->count_account_roles(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_roles(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view path_prefix,
				   std::string_view marker,
				   uint32_t max_items,
				   RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "account",
      "", std::string(account_id),
      std::string(path_prefix), std::string(marker),
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int NSFSDriver::list_roles(const DoutPrefixProvider *dpp,
			   optional_yield y,
			   const std::string& tenant,
			   const std::string& path_prefix,
			   const std::string& marker,
			   uint32_t max_items,
			   RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "tenant",
      tenant, "",
      path_prefix, marker,
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int NSFSDriver::load_account_user_by_name(const DoutPrefixProvider* dpp,
					  optional_yield y,
					  std::string_view account_id,
					  std::string_view tenant,
					  std::string_view username,
					  std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_account_user_by_name(dpp,
      std::string(account_id), std::string(username), uinfo);
  if (ret < 0) {
    return ret;
  }
  if (user) {
    *user = get_user(uinfo.user_id);
    (*user)->get_info() = uinfo;
    ret = (*user)->load_user(dpp, y);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int NSFSDriver::count_account_users(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    uint32_t& count)
{
  return get_user_db()->count_account_users(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_users(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view tenant,
				   std::string_view path_prefix,
				   std::string_view marker,
				   uint32_t max_items,
				   UserList& listing)
{
  std::vector<RGWUserInfo> users;
  int ret = get_user_db()->list_account_users(dpp,
      std::string(account_id), std::string(marker),
      max_items + 1, users);
  if (ret < 0) {
    return ret;
  }

  if (!path_prefix.empty()) {
    std::string pp(path_prefix);
    users.erase(
        std::remove_if(users.begin(), users.end(),
            [&pp](const RGWUserInfo& u) {
              return u.path.substr(0, pp.size()) != pp;
            }),
        users.end());
  }

  if (users.size() > max_items) {
    listing.next_marker = users[max_items].display_name;
    users.resize(max_items);
  }
  listing.users = std::move(users);
  return 0;
}

int NSFSDriver::load_group_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y, std::string_view id,
				 RGWGroupInfo& info, Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  info.id = std::string(id);
  return get_user_db()->get_group(dpp, "group_id", info, attrs);
}

int NSFSDriver::load_group_by_name(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view name,
				   RGWGroupInfo& info, Attrs& attrs,
				   RGWObjVersionTracker& objv)
{
  info.account_id = std::string(account_id);
  info.name = std::string(name);
  return get_user_db()->get_group(dpp, "name", info, attrs);
}

int NSFSDriver::store_group(const DoutPrefixProvider* dpp, optional_yield y,
			    const RGWGroupInfo& info, const Attrs& attrs,
			    RGWObjVersionTracker& objv, bool exclusive,
			    const RGWGroupInfo* old_info)
{
  return get_user_db()->store_group(dpp, info, attrs, exclusive);
}

int NSFSDriver::remove_group(const DoutPrefixProvider* dpp, optional_yield y,
			     const RGWGroupInfo& info,
			     RGWObjVersionTracker& objv)
{
  return get_user_db()->remove_group(dpp, info);
}

int NSFSDriver::list_group_users(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view id,
				 std::string_view marker,
				 uint32_t max_items,
				 UserList& listing)
{
  std::vector<std::string> user_ids;
  int ret = get_user_db()->list_group_users(dpp,
      std::string(id), std::string(marker), max_items + 1, user_ids);
  if (ret < 0) {
    return ret;
  }

  if (user_ids.size() > max_items) {
    listing.next_marker = user_ids[max_items];
    user_ids.resize(max_items);
  }

  for (auto& uid : user_ids) {
    RGWUserInfo uinfo;
    uinfo.user_id.id = uid;
    ret = get_user_db()->get_user(dpp, std::string("user_id"), uid,
                                  uinfo, nullptr, nullptr);
    if (ret < 0) {
      continue;
    }
    listing.users.push_back(std::move(uinfo));
  }
  return 0;
}

int NSFSDriver::count_account_groups(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     uint32_t& count)
{
  return get_user_db()->count_account_groups(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_groups(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view path_prefix,
				    std::string_view marker,
				    uint32_t max_items,
				    GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = get_user_db()->list_account_groups(dpp,
      std::string(account_id), std::string(path_prefix),
      std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int NSFSDriver::store_oidc_provider(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    const RGWOIDCProviderInfo& info,
				    bool exclusive,
				    RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->store_oidc_provider(dpp, info, exclusive);
}

int NSFSDriver::load_oidc_provider(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view tenant,
				   std::string_view url,
				   RGWOIDCProviderInfo& info,
				   RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->load_oidc_provider(dpp,
      std::string(tenant), std::string(url), info);
}

int NSFSDriver::delete_oidc_provider(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view tenant,
				     std::string_view url)
{
  return get_user_db()->delete_oidc_provider(dpp,
      std::string(tenant), std::string(url));
}

int NSFSDriver::get_oidc_providers(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view tenant,
				   std::vector<RGWOIDCProviderInfo>& providers)
{
  return get_user_db()->list_oidc_providers(dpp,
      std::string(tenant), providers);
}

/* --- Notification publish methods --- */

int NSFSNotification::publish_reserve(const DoutPrefixProvider *dpp,
				      RGWObjTags* obj_tags)
{
  obj_tags_ptr = obj_tags;

  if (!bucket) {
    return 0;
  }

  int ret = get_bucket_notifications(dpp, bucket, bucket_topics);
  if (ret < 0) {
    return ret;
  }

  const std::string obj_name = obj ? obj->get_name() : "";

  for (auto& [name, filter] : bucket_topics.topics) {
    bool event_match = false;
    for (auto req_type : event_types) {
      for (auto cfg_type : filter.events) {
	if (static_cast<uint64_t>(req_type) & static_cast<uint64_t>(cfg_type)) {
	  event_match = true;
	  break;
	}
      }
      if (event_match) break;
    }
    if (!event_match) continue;

    if (!match(filter.s3_filter.key_filter, obj_name)) {
      continue;
    }

    if (!filter.s3_filter.metadata_filter.kv.empty()) {
      if (!match(filter.s3_filter.metadata_filter, x_meta_map)) {
	continue;
      }
    }

    if (!filter.s3_filter.tag_filter.kv.empty()) {
      KeyMultiValueMap tags;
      if (obj_tags) {
	tags = obj_tags->get_tags();
      }
      if (!match(filter.s3_filter.tag_filter, tags)) {
	continue;
      }
    }

    matched.push_back(filter);
  }

  return 0;
}

int NSFSNotification::publish_commit(const DoutPrefixProvider* dpp,
				     uint64_t size,
				     const ceph::real_time& mtime,
				     const std::string& etag,
				     const std::string& version)
{
  if (matched.empty()) {
    return 0;
  }

  for (auto& filter : matched) {
    const auto& dest = filter.topic.dest;
    if (dest.push_endpoint.empty()) {
      continue;
    }

    rgw_pubsub_s3_event event;
    event.eventTime = mtime;
    event.eventName = rgw::notify::to_string(event_types.front());
    event.userIdentity = user_id;
    event.x_amz_request_id = req_id;
    event.configurationId = filter.s3_id;
    if (bucket) {
      event.bucket_name = bucket->get_name();
      event.bucket_ownerIdentity = to_string(bucket->get_owner());
      event.bucket_id = bucket->get_bucket_id();
    }
    if (obj) {
      event.object_key = obj->get_name();
    }
    event.object_size = size;
    event.object_etag = etag;
    event.object_versionId = version;

    try {
      RGWHTTPArgs args(dest.push_endpoint_args, dpp);
      auto endpoint = RGWPubSubEndpoint::create(
	  dest.push_endpoint, filter.topic.name, args,
	  dpp->get_cct());
      int r = endpoint->send(dpp, event, null_yield);
      if (r < 0) {
	ldpp_dout(dpp, 1) << "ERROR: notification endpoint send failed: "
			  << dest.push_endpoint << " ret=" << r << dendl;
      }
    } catch (const RGWPubSubEndpoint::configuration_error& e) {
      ldpp_dout(dpp, 1) << "ERROR: notification endpoint config error: "
			<< e.what() << dendl;
    }
  }

  return 0;
}

/* --- Topic SAL methods --- */

int NSFSDriver::read_topic_v2(const std::string& topic_name,
			      const std::string& tenant,
			      rgw_pubsub_topic& topic,
			      RGWObjVersionTracker* objv_tracker,
			      optional_yield y,
			      const DoutPrefixProvider* dpp)
{
  obj_version objv;
  int ret = get_user_db()->load_topic(dpp, topic_name, tenant, topic, objv);
  if (!ret && objv_tracker) {
    objv_tracker->read_version = objv;
  }
  return ret;
}

int NSFSDriver::write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
			       RGWObjVersionTracker& objv_tracker,
			       optional_yield y,
			       const DoutPrefixProvider* dpp)
{
  return get_user_db()->store_topic(dpp, topic, exclusive, objv_tracker.write_version);
}

int NSFSDriver::remove_topic_v2(const std::string& topic_name,
				const std::string& tenant,
				RGWObjVersionTracker& objv_tracker,
				optional_yield y,
				const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_topic(dpp, topic_name, tenant);
}

int NSFSDriver::update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					    const std::string& bucket_key,
					    bool add_mapping,
					    optional_yield y,
					    const DoutPrefixProvider* dpp)
{
  if (add_mapping) {
    return get_user_db()->add_bucket_topic_mapping(dpp, topic.name, bucket_key);
  } else {
    return get_user_db()->remove_bucket_topic_mapping(dpp, topic.name, bucket_key);
  }
}

int NSFSDriver::get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					 std::set<std::string>& bucket_keys,
					 optional_yield y,
					 const DoutPrefixProvider* dpp)
{
  return get_user_db()->get_bucket_topic_mapping(dpp, topic.name, bucket_keys);
}

int NSFSDriver::remove_bucket_mapping_from_topics(
    const rgw_pubsub_bucket_topics& bucket_topics,
    const std::string& bucket_key,
    optional_yield y,
    const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_bucket_from_topic_mappings(dpp, bucket_key);
}

int NSFSDriver::list_account_topics(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view marker,
				    uint32_t max_items,
				    TopicList& listing)
{
  rgw_owner owner = rgw_account_id(std::string(account_id));
  std::vector<rgw_pubsub_topic> topics;
  int ret = get_user_db()->list_topics(dpp, "owner", owner,
      std::string(marker), max_items, topics);
  if (ret) {
    return ret;
  }
  for (auto& t : topics) {
    listing.topics.push_back(std::move(t.name));
  }
  if (!listing.topics.empty()) {
    listing.next_marker = listing.topics.back();
  }
  return 0;
}

struct meta_list_handle {
  std::string marker;
  std::string section;

  DIR *dir = nullptr;
  long dpos = -1;

  meta_list_handle(const std::string& _section, const std::string& _marker) {
    marker = _marker;
    section = _section;
  }
};

int NSFSDriver::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                     const std::string& section,
                                     const std::string& marker, void** phandle)
{
  meta_list_handle* stuff = new meta_list_handle(section, marker);
  *phandle = (void *)stuff;
  if (section == "bucket") {
    int ret;
    int dfd = copy_dir_fd(get_root_fd());
    if (dfd == -1) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
                        << cpp_strerror(errno) << dendl;
      return -ret;
    }

    stuff->dir = fdopendir(dfd);
    if (stuff->dir == NULL) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not open root to list buckets: "
                        << cpp_strerror(ret) << dendl;
      ::close(dfd);
      return -ret;
    }
  }
  return 0;
  }

int NSFSDriver::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
                                     int max, std::list<std::string>& keys,
                                     bool* truncated)
{
  meta_list_handle *h = static_cast<meta_list_handle *>(handle);
  *truncated = false;
  int ret;
  keys.clear();
  if (h->section == "user") {
    ret = get_user_db()->list_users(dpp, h->marker, max, keys, truncated);
    if (ret < 0) {
      return ret;
    }
    if (keys.size() > 0) {
      h->marker = *keys.rbegin();
      if (std::cmp_equal(keys.size(),max)) {
        *truncated = true;
      }
    }
  } else if (h->section == "bucket") {
    if (h->dpos != -1) {
      seekdir(h->dir, h->dpos);
    }
    struct dirent* entry;
    while ((entry = readdir(h->dir)) != NULL) {
      if (entry->d_type == DT_UNKNOWN) {
        struct statx stx;

        ret = statx(get_root_fd(), entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
        if (ret < 0) {
          ret = errno;
          ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name << ": "
	                    << cpp_strerror(ret) << dendl;
          return -ret;
        }
        if (!S_ISDIR(stx.stx_mode)) {
        /* Not a bucket, skip it */
          continue;
        }
      } else if (entry->d_type != DT_DIR) {
        continue;
      }
      if (entry->d_name[0] == '.') {
        /* Skip dotfiles */
        continue;
     }
      keys.push_back(entry->d_name);
      if (std::cmp_equal(keys.size(),max)) {
        h->dpos = telldir(h->dir);
        *truncated = true;
        break;
      }
    }
  }
  return 0;
}

void NSFSDriver::meta_list_keys_complete(void* handle)
{
  if (handle) {
    meta_list_handle *h = static_cast<meta_list_handle *>(handle);
    if (h->section == "bucket") {
      closedir(h->dir);
    }
    delete h;
  }
  return;
}

MDB_cmp_func* NSFSBucket::lmdb_cmp()
{
  return nsfs_lmdb_cmp;
}

int NSFSBucket::fill_cache(const DoutPrefixProvider* dpp, optional_yield y,
                            fill_cache_cb_t& cb)
{
  uint32_t flags = nsfs::FSEnt::FLAG_NONE;
  if (get_info().versioned()) {
    flags |= nsfs::FSEnt::FLAG_LIST_VERSIONS;
  }
  return dir->fill_cache(dpp, y, cb, flags);
}

int NSFSBucket::list(const DoutPrefixProvider* dpp, ListParams& params,
		    int max, ListResults& results, optional_yield y)
{
  /* multipart namespace: incomplete uploads live in staging directories
   * that aren't in the LMDB cache — delegate to list_multiparts() and
   * format results using RADOS naming conventions so the LC processor
   * can parse them via rgw_obj_key::parse_index_key().
   *
   * Skip this when we ARE a shadow (staging) bucket (ns == mp_ns) —
   * list_parts() uses shadow->list() to enumerate part files inside
   * the staging directory, which must fall through to the normal
   * LMDB/directory listing below. */
  if (params.ns == mp_ns && ns != mp_ns) {
    std::vector<std::unique_ptr<MultipartUpload>> uploads;
    std::string marker;
    int ret = list_multiparts(dpp, "", marker, "",
			      max, uploads, nullptr,
			      &results.is_truncated, y);
    if (ret < 0)
      return ret;
    for (auto& upload : uploads) {
      const auto& obj_name = upload->get_key();
      if (!params.prefix.empty() &&
	  !obj_name.starts_with(params.prefix)) {
	continue;
      }
      rgw_bucket_dir_entry bde{};
      bde.key.name = fmt::format("_{}_{}",
				 mp_ns,
				 upload->get_meta());
      bde.meta.mtime = upload->get_mtime();
      bde.meta.category = RGWObjCategory::MultiMeta;
      bde.exists = true;
      results.objs.push_back(std::move(bde));
    }
    return 0;
  }

int count{0};
bool in_prefix{false};
// Names in the cache are in OID format
rgw_obj_key marker_key(params.marker);
params.marker = marker_key.get_oid();
{
  rgw_obj_key key(params.prefix);
  params.prefix = key.name;
}
if (max <= 0) {
    return 0;
  }

  //params.list_versions
  int ret = driver->get_bucket_cache()->list_bucket(
    dpp, y, this, params.marker.name, [&](const rgw_bucket_dir_entry& bde) -> bool
      {
	std::string ns;
	// bde.key can be encoded with the namespace.  Decode it here
	rgw_obj_key bde_key{bde.key};
	if (!params.list_versions && !bde.is_visible()) {
	  return true;
	}
        if (bde_key.ns != params.ns) {
          // Namespace must match
          return true;
        }
        if (!marker_key.empty() && marker_key == bde_key.name) {
	  // Skip marker
	  return true;
	}
	if (!params.prefix.empty()) {
	  // We have a prefix, only match
          if (!bde_key.name.starts_with(params.prefix)) {
            // Prefix doesn't match; skip
	    if (in_prefix) {
              return false;
            }
            return true;
          }
	  // Prefix matches
	  if (params.delim.empty()) {
	    // No delimiter, add matches
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          auto delim_pos = bde_key.name.find(params.delim, params.prefix.size());
          if (delim_pos == std::string_view::npos) {
	    // Straight prefix match
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
	  }
          results.next_marker =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!results.common_prefixes.contains(results.next_marker.name)) {
            results.common_prefixes[results.next_marker.name] = true;
            count++; // Count will be checked when we exit prefix
            if (in_prefix) {
              // We've hit the next prefix entry.  Check count
              if (count >= max) {
                results.is_truncated = true;
                // Time to stop
                return false;
	      }
            }
          }
          in_prefix = true;
          return true;
        }
        if (!params.delim.empty()) {
	  // Delimiter, but no prefix
	  auto delim_pos = bde_key.name.find(params.delim) ;
          if (delim_pos == std::string_view::npos) {
	    // Delimiter doesn't match, insert
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          std::string prefix_key =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!marker_key.empty() && marker_key == prefix_key) {
            // Skip marker
            return true;
          }
	  std::string decoded_key;
	  rgw_obj_key::parse_index_key(prefix_key, &decoded_key, &ns);
          if (!results.common_prefixes.contains(decoded_key)) {
	    if (in_prefix) {
	      // New prefix, check the count
	      count++;
              if (count >= max) {
                results.is_truncated = true;
                return false;
              }
            }
	    in_prefix = true;
            results.common_prefixes[decoded_key] = true;
	    // Fallthrough
          }
	  results.next_marker.name = decoded_key;
	  return true;
        }

        results.next_marker.set(bde.key);
        results.objs.push_back(bde);
        count++;
        if (count >= max) {
          results.is_truncated = true;
          return false;
        }
        return true;
    });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    results.objs.clear();
    return ret;
  }

  return 0;
}

int NSFSBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  for (auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }

  return write_attrs(dpp, y);
}

int NSFSBucket::remove(const DoutPrefixProvider* dpp,
			bool delete_children,
			optional_yield y)
{
  int ret = dir->remove(dpp, y, delete_children);
  if (ret < 0) {
    return ret;
  }

  driver->get_bucket_cache()->invalidate_bucket(dpp, get_name());

  return ret;
}

int NSFSBucket::remove_bypass_gc(int concurrent_max,
				  bool keep_index_consistent,
				  optional_yield y,
				  const DoutPrefixProvider *dpp)
{
  return remove(dpp, true, y);
}

int NSFSBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  if (get_name()[0] == '.') {
    /* Skip dotfiles */
    return -ERR_INVALID_OBJECT_NAME;
  }
  ret = dir->stat(dpp);
  if (ret < 0) {
    return ret;
  }

  mtime = ceph::real_clock::from_time_t(dir->get_stx().stx_mtime.tv_sec);
  info.creation_time = ceph::real_clock::from_time_t(dir->get_stx().stx_btime.tv_sec);

  ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = dir->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  RGWBucketInfo bak_info = info;;
  ret = decode_attr(attrs, RGW_NSFS_ATTR_BUCKET_INFO, info);
  if (ret < 0) {
    // TODO dang: fake info up (UID to owner conversion?)
    info = bak_info;
  } else {
    // Don't leave info visible in attributes
    attrs.erase(RGW_NSFS_ATTR_BUCKET_INFO);
  }

  return 0;
}

int NSFSBucket::set_acl(const DoutPrefixProvider* dpp,
			 RGWAccessControlPolicy& acl,
			 optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  info.owner = acl.get_owner().id;

  return write_attrs(dpp, y);
}

int NSFSBucket::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
			    const bucket_index_layout_generation& idx_layout,
			    int shard_id, std::string* bucket_ver, std::string* master_ver,
			    std::map<RGWObjCategory, RGWStorageStats>& stats,
			    std::string* max_marker, bool* syncstopped)
{
  auto& main = stats[RGWObjCategory::Main];

  // TODO: bucket stats shouldn't have to list all objects
  return dir->for_each(dpp, [this, dpp, y, &main] (const char* name) {
    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    std::unique_ptr<FSEnt> dent;
    int ret = dir->get_ent(dpp, y, name, std::string(), dent);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get ent for object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    ret = dent->stat(dpp);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    struct statx& lstx = dent->get_stx();

    if (S_ISREG(lstx.stx_mode) || S_ISDIR(lstx.stx_mode)) {
      main.num_objects++;
      main.size += lstx.stx_size;
      main.size_rounded += lstx.stx_size;
      main.size_utilized += lstx.stx_size;
    }

    return 0;
  });
  return 0;
}

int NSFSBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return 0;
}

int NSFSBucket::sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                  RGWBucketEnt* ent)
{
  return 0;
}

int NSFSBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                     uint64_t num_objs, optional_yield y)
{
  return 0;
}

int NSFSBucket::chown(const DoutPrefixProvider* dpp,
                       const rgw_owner& new_owner,
                       const std::string& new_owner_name,
                       optional_yield y) {
  /* TODO map user to UID/GID, and change it */
  return 0;
}

int NSFSBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
{
  mtime = _mtime;

  struct timespec ts[2];
  ts[0].tv_nsec = UTIME_OMIT;
  ts[1] = ceph::real_clock::to_timespec(mtime);
  int ret = utimensat(dir->get_parent()->get_fd(), get_fname().c_str(), ts, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not set mtime on bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return write_attrs(dpp, y);
}

int NSFSBucket::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  encode(info, bl);
  Attrs extra_attrs;
  extra_attrs[RGW_NSFS_ATTR_BUCKET_INFO] = bl;

  return dir->write_attrs(dpp, y, attrs, &extra_attrs);
}

int NSFSBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return dir->for_each(dpp, [](const char* name) {
    /* for_each filters out "." and "..", so reaching here is not empty */
    std::string_view check_name = name;
    if (!check_name.starts_with(".multipart")) { // incomplete uploads can be deleted
      return -ENOTEMPTY;
    }
    return 0;
  });
}

int NSFSBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
  return driver->get_quota_handler()->check_quota(dpp, info.owner, get_key(),
                                                  quota, (check_size_only ? 0 : 1),
                                                  obj_size, y);
}

int NSFSBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y)
{
  *pmtime = mtime;

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return dir->read_attrs(dpp, y, attrs);
}

int NSFSBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			    uint64_t end_epoch, uint32_t max_entries,
			    bool* is_truncated, RGWUsageIter& usage_iter,
			    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int NSFSBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  return 0;
}

int NSFSBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return 0;
}

int NSFSBucket::check_index(const DoutPrefixProvider *dpp, optional_yield y,
                             std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                             std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return 0;
}

int NSFSBucket::rebuild_index(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int NSFSBucket::set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout)
{
  return 0;
}

int NSFSBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

std::unique_ptr<MultipartUpload> NSFSBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<NSFSMultipartUpload>(driver, this, oid, upload_id, owner, mtime);
}

int NSFSBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated, optional_yield y)
{
  int count = 0;
  int ret;

  ret = dir->for_each(dpp, [this, dpp, y, &count, &max_uploads, &is_truncated, &uploads] (const char* name) {
    std::string_view d_name = name;
    static std::string mp_pre{"." + mp_ns + "_"};
    if (!d_name.starts_with(mp_pre)) {
      /* Skip non-uploads */
      return 0;
    }

    if (count >= max_uploads) {
      if (is_truncated) {
	*is_truncated = true;
      }

      return -EAGAIN;
    }

    d_name.remove_prefix(mp_pre.size());

    /* use the staging directory's mtime as the upload creation time */
    struct statx stx;
    if (statx(dir->get_fd(), name, AT_SYMLINK_NOFOLLOW, STATX_MTIME, &stx) < 0) {
      return 0;
    }
    auto mtime = from_statx_timestamp(stx.stx_mtime);

    ACLOwner owner;
    std::string upload_id{d_name};
    std::unique_ptr<MultipartUpload> upload =
        std::make_unique<NSFSMultipartUpload>(
            driver, this, std::string(), upload_id, owner,
            mtime);
    rgw_placement_rule* rule{nullptr};
    int ret = upload->get_info(dpp, y, &rule, nullptr);
    if (ret < 0)
      return 0;
    uploads.emplace(uploads.end(), std::move(upload));
    count++;

    return 0;
  });

  return ret;
}

int NSFSBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y)
{
  return 0;
}

int NSFSBucket::create(const DoutPrefixProvider* dpp, optional_yield y, bool* existed)
{
  int ret = dir->create(dpp, existed);
  if (ret < 0) {
    return ret;
  }

  return write_attrs(dpp, y);
}

std::string NSFSBucket::get_fname()
{
  return bucket_fname(get_name(), ns);
}

int NSFSBucket::rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj)
{
  int ret;
  Directory* dst_dir = dir->get_parent();

  info.bucket.name = target_obj->get_key().get_oid();
  ns.reset();

  if (!target_obj->get_instance().empty()) {
    /* This is a versioned object.  Need to handle versioneddirectory */
    NSFSObject *to = static_cast<NSFSObject *>(target_obj);
    ret = to->open(dpp, true, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open target obj " << to->get_name() << dendl;
      return ret;
    }
    dst_dir = static_cast<Directory *>(to->get_fsent());
  }

  return dir->rename(dpp, y, dst_dir, get_fname());
}

int NSFSObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				uint32_t flags,
                                std::list<rgw_obj_index_key>* remove_objs,
				RGWObjVersionTracker* objv)
{
  NSFSBucket *b = static_cast<NSFSBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = stat(dpp);
  if (ret < 0) {
      if (ret == -ENOENT) {
	// Nothing to do
	return 0;
      }
      return ret;
  }

  ret = ent->remove(dpp, y, /*delete_children=*/false);
  if (ret < 0)
    return ret;

  for (auto it = dir_chain.rbegin(); it != dir_chain.rend(); ++it) {
    int r = (*it)->remove(dpp, y, /*delete_children=*/false);
    if (r < 0)
      break;
  }
  dir_chain.clear();

  cls_rgw_obj_key key;
  get_key().get_index_key(&key);

  driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);

  if (!key.instance.empty() && !ent->exists()) {
    /* Remove the non-versioned key as well */
    key.instance.clear();
    driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);
  }
  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            -1, 0, state.accounted_size);
  return 0;
}

int NSFSObject::copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              rgw::sal::DataProcessorFactory* dp_factory,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  int ret;
  NSFSBucket *db = static_cast<NSFSBucket*>(dest_bucket);
  NSFSBucket *sb = static_cast<NSFSBucket*>(src_bucket);
  NSFSObject *dobj = static_cast<NSFSObject*>(dest_object);

  if (!db || !sb) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket to copy " << get_name()
                      << dendl;
    return -EINVAL;
  }
  bool has_instance = !get_key().instance.empty();

  ldpp_dout(dpp, 10) << "copy_object: src=" << get_key()
    << " dst=" << dobj->get_key()
    << " src_bucket=" << sb->get_name()
    << " dst_bucket=" << db->get_name() << dendl;

  // Source must exist, and we need to know if it's a shadow obj
  if (!check_exists(dpp)) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  /* check copy-source preconditions against the source object */
  if (if_match) {
    std::string if_match_str = rgw_string_unquote(if_match);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (mod_ptr && state.mtime <= *mod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }
  if (unmod_ptr && state.mtime > *unmod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }

  if (!get_key().instance.empty() && !has_instance) {
    /* For copy, no instance meance copy all instances.  Clear intance id if it
     * was passed in clear. */
    get_key().instance.clear();
  }

  const auto& dbinfo = db->get_info();
  bool dest_versioned = dbinfo.versioned();
  bool dest_ver_enabled = dbinfo.versioning_enabled();
  std::string demoted_ver_id;
  bool did_demote = false;
  std::unique_ptr<VersionLockHandle> vlock;

  /* versioned copy: demote existing dest current to .versions/
   * (includes self-copy — S3 requires a new version even when
   * source == dest with MetadataDirective=REPLACE) */
  if (dest_versioned) {
    /* stat destination to find existing current version */
    dobj->ent.reset();
    int dret = dobj->stat(dpp);
    nsfs::FSEnt* dest_ent = dobj->get_fsent();

    int dest_parent_fd = -1;
    std::string dest_leaf;
    if (dest_ent && dest_ent->get_parent()) {
      nsfs::Directory* dp = dest_ent->get_parent();
      dest_parent_fd = dp->get_fd();
      if (dest_parent_fd < 0) {
        dp->open(dpp);
        dest_parent_fd = dp->get_fd();
      }
      dest_leaf = dest_ent->get_name();
    }

    if (dest_parent_fd >= 0) {
      vlock = driver->get_fs_strategy()->version_lock(
        dpp, open_versions_lockfile(dest_parent_fd));

      int vfd = open_versions_dir(dest_parent_fd);
      if (vfd >= 0) {
        if (!dest_ver_enabled) {
          std::string null_name = nsfs_ver_entry(dest_leaf, NULL_VERSION_ID);
          ::unlinkat(vfd, null_name.c_str(), 0);
        }

        /* re-stat under lock for fresh state */
        struct statx cur_stx;
        if (dret == 0 && dest_ent && dest_ent->exists() &&
            statx(dest_parent_fd, dest_leaf.c_str(),
                  AT_SYMLINK_NOFOLLOW, STATX_ALL, &cur_stx) == 0) {
          bool cur_is_null = false;
          {
            int chk_fd = ::openat(dest_parent_fd, dest_leaf.c_str(), O_RDONLY);
            if (chk_fd >= 0) {
              cur_is_null = is_null_version_fd(chk_fd);
              ::close(chk_fd);
            }
          }
          if (!(!dest_ver_enabled && cur_is_null)) {
            std::string cur_ver_id = cur_is_null
              ? NULL_VERSION_ID
              : nsfs_version_id_from_statx(cur_stx);
            std::string ver_name = nsfs_ver_entry(dest_leaf, cur_ver_id);

            SafeResult sr = driver->get_fs_strategy()->safe_link(
              dpp, dest_parent_fd, dest_leaf,
              vfd, ver_name,
              statx_mtime_ns(cur_stx), cur_stx.stx_ino);
            if (sr == SafeResult::OK) {
              int demoted_fd = ::openat(vfd, ver_name.c_str(), O_RDONLY);
              if (demoted_fd >= 0) {
                auto now_ms = std::to_string(
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                std::string ts_x =
                  NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_NON_CURRENT_TS;
                ::fsetxattr(demoted_fd, ts_x.c_str(),
                            now_ms.c_str(), now_ms.size(), 0);
                ::close(demoted_fd);
              }
              demoted_ver_id = cur_ver_id;
              did_demote = true;
            }
          }
        }
        ::close(vfd);
      }
    }
  }

  bool same_key = (get_bucket()->get_name() == dobj->get_bucket()->get_name() &&
                   get_key().name == dobj->get_key().name);
  if (!same_key) {
    /* cross-object copy */
    ret = copy(dpp, y, sb, db, dobj);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to copy object " << get_key()
                          << dendl;
        return ret;
    }
  } else if (did_demote && ent) {
    rgw_obj_key dst_key = dobj->get_key();
    std::vector<std::unique_ptr<nsfs::Directory>> dst_chain;
    nsfs::Directory* dst_leaf_dir = nullptr;
    std::string dst_leaf_name;
    ret = nsfs::resolve_path(dpp, db->get_dir(),
        get_key_fname(dst_key, /*use_version=*/false),
        /*create_dirs=*/true, driver->ctx(),
        dst_chain, dst_leaf_dir, dst_leaf_name);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: copy_object: self-copy resolve_path failed: "
        << cpp_strerror(-ret) << dendl;
      return ret;
    }
    int src_fd = ent->get_fd();
    if (src_fd < 0) {
      ent->open(dpp);
      src_fd = ent->get_fd();
    }
    ret = driver->get_fs_strategy()->clone_fd(
      dpp, src_fd, dst_leaf_dir->get_fd(), dst_leaf_name);
    if (ret < 0) {
      return ret;
    }
  }
  vlock.reset();
  dobj->make_ent(ent->get_type());

  /* Set up attributes for destination */
  Attrs src_attrs = state.attrset;
  /* Come attrs are never copied */
  src_attrs.erase(RGW_ATTR_DELETE_AT);
  src_attrs.erase(RGW_ATTR_OBJECT_RETENTION);
  src_attrs.erase(RGW_ATTR_OBJECT_LEGAL_HOLD);
  /* Some attrs, if they exist, always come from the call */
  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  bufferlist rt;
  if (get_attr(RGW_ATTR_OBJECT_RETENTION, rt)) {
    src_attrs[RGW_ATTR_OBJECT_RETENTION] = rt;
  }
  bufferlist lh;
  if (get_attr(RGW_ATTR_OBJECT_LEGAL_HOLD, lh)) {
    src_attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = lh;
  }

  bufferlist tt;
  switch (attrs_mod) {
  case ATTRSMOD_REPLACE:
    /* Keep tags if not set */
    if (!attrs[RGW_ATTR_ETAG].length()) {
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];
    }
    if (!attrs[RGW_ATTR_TAIL_TAG].length() &&
	rgw::sal::get_attr(src_attrs, RGW_ATTR_TAIL_TAG, tt)) {
      attrs[RGW_ATTR_TAIL_TAG] = tt;
    }
    break;

  case ATTRSMOD_MERGE:
    for (auto it = src_attrs.begin(); it != src_attrs.end(); ++it) {
      if (attrs.find(it->first) == attrs.end()) {
	attrs[it->first] = it->second;
      }
    }
    break;
  case ATTRSMOD_NONE:
    attrs = src_attrs;
    ret = 0;
    break;
  }

  /* Some attrs always come from the source */
  bufferlist com;
  if (rgw::sal::get_attr(src_attrs, RGW_ATTR_COMPRESSION, com)) {
    attrs[RGW_ATTR_COMPRESSION] = com;
  }
  bufferlist mpu;
  if (rgw::sal::get_attr(src_attrs, RGW_NSFS_ATTR_MPUPLOAD, mpu)) {
    attrs[RGW_NSFS_ATTR_MPUPLOAD] = mpu;
  }
  bufferlist pot;
  if (rgw::sal::get_attr(src_attrs, RGW_NSFS_ATTR_OBJECT_TYPE, pot)) {
    attrs[RGW_NSFS_ATTR_OBJECT_TYPE] = pot;
  }
  ret = dobj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
  if (ret < 0) {
    return ret;
  }

  /* versioned copy: compute version ID and update cache */
  if (dest_versioned && dobj->get_fsent()) {
    dobj->get_fsent()->stat(dpp, /*force=*/true);
    const struct statx& new_stx = dobj->get_fsent()->get_stx();
    std::string new_ver_id = dest_ver_enabled
      ? nsfs_version_id_from_statx(new_stx)
      : NULL_VERSION_ID;
    dest_object->set_instance(new_ver_id);
    if (version_id) {
      *version_id = new_ver_id;
    }
    int obj_fd = dobj->get_fsent()->get_fd();
    if (obj_fd >= 0) {
      std::string vid_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
      ::fsetxattr(obj_fd, vid_xattr.c_str(),
                  new_ver_id.c_str(), new_ver_id.size(), 0);
    }

    auto* bcache = driver->get_bucket_cache();
    std::string obj_name = dobj->get_key().get_index_key_name();

    if (did_demote) {
      cls_rgw_obj_key old_key;
      old_key.name = obj_name;
      old_key.instance = demoted_ver_id;
      bcache->remove_entry(dpp, db->get_name(), old_key);

      rgw_bucket_dir_entry dem_bde{};
      dem_bde.key.name = obj_name;
      dem_bde.key.instance = demoted_ver_id;
      dem_bde.ver.pool = 1;
      dem_bde.ver.epoch = 1;
      dem_bde.exists = true;
      dem_bde.meta.category = RGWObjCategory::Main;
      dem_bde.flags = rgw_bucket_dir_entry::FLAG_VER;
      bcache->add_entry(dpp, db->get_name(), dem_bde);
    }

    rgw_bucket_dir_entry new_bde{};
    new_bde.key.name = obj_name;
    new_bde.key.instance = new_ver_id;
    new_bde.ver.pool = 1;
    new_bde.ver.epoch = 1;
    new_bde.exists = true;
    new_bde.meta.category = RGWObjCategory::Main;
    new_bde.meta.size = new_stx.stx_size;
    new_bde.meta.accounted_size = new_stx.stx_size;
    new_bde.meta.mtime = from_statx_timestamp(new_stx.stx_mtime);
    new_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    new_bde.meta.etag = synthesize_etag(new_stx);
    new_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                    rgw_bucket_dir_entry::FLAG_CURRENT;
    bcache->add_entry(dpp, db->get_name(), new_bde);
  }

  return 0;
}

int NSFSObject::list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			    int max_parts, int marker, int* next_marker,
			    bool* truncated, list_parts_each_t&& each_func,
			    optional_yield y)
{
  *truncated = false;

  std::vector<uint64_t> part_sizes;
  if (!decode_raw_attr(state.attrset, RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes)) {
    return 0;
  }

  int nparts = part_sizes.size();
  int start = marker;
  int emitted = 0;

  for (int i = start; i < nparts; ++i) {
    if (emitted >= max_parts) {
      *truncated = true;
      break;
    }
    Part part;
    part.part_number = i + 1;
    part.part_size = part_sizes[i];
    int ret = each_func(part);
    if (ret < 0) {
      return ret;
    }
    *next_marker = i + 1;
    ++emitted;
  }

  return 0;
}

bool NSFSObject::is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                                    const ceph::real_time& obj_mtime)
{
  return false;
}

int NSFSObject::load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh)
{
  int ret = stat(dpp);
  if (ret < 0) {
    if (state.is_dm && !dm_version_id.empty()) {
      state.obj.key.instance = dm_version_id;
    }
    return ret;
  }

  ret = get_obj_attrs(y, dpp);

  return ret;
}

int NSFSObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  if (delattrs) {
    for (auto& it : *delattrs) {
      if (it.first == RGW_NSFS_ATTR_OBJECT_TYPE) {
	// Don't delete type
	continue;
      }
      state.attrset.erase(it.first);
    }
  }
  if (setattrs) {
    for (auto& it : *setattrs) {
      if (it.first == RGW_NSFS_ATTR_OBJECT_TYPE) {
	// Don't overwrite type
	continue;
      }
      state.attrset[it.first] = it.second;
    }
  }

  write_attrs(dpp, y);
  return 0;
}

int NSFSObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = open(dpp, false);
  if (ret < 0) {
    return ret;
  }

  ret = ent->read_attrs(dpp, y, state.attrset);
  if (ret == 0)
    state.has_attrs = true;
  else
    state.has_attrs = false;

  if (state.has_attrs && state.attrset.find(RGW_ATTR_ETAG) == state.attrset.end()) {
    bufferlist bl;
    std::string etag = synthesize_etag(ent->get_stx());
    bl.append(etag);
    state.attrset[RGW_ATTR_ETAG] = std::move(bl);
  }

  if (state.has_attrs && state.attrset.find(RGW_ATTR_CONTENT_TYPE) == state.attrset.end()) {
    std::string name = ent->get_name();
    auto dot = name.rfind('.');
    if (dot != std::string::npos && dot < name.size() - 1) {
      auto mime = rgw_find_mime_by_ext(name.substr(dot + 1));
      if (!mime.empty()) {
        bufferlist bl;
        bl.append(mime.data(), mime.size());
        state.attrset[RGW_ATTR_CONTENT_TYPE] = std::move(bl);
      }
    }
  }

  return ret;
}

int NSFSObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp, uint32_t flags)
{
  state.attrset[attr_name] = attr_val;
  return write_attrs(dpp, y);
}

int NSFSObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y)
{
  state.attrset.erase(attr_name);

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = remove_x_attr(dpp, y, ent->get_fd(), attr_name, get_name());
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remover attribute " << attr_name << " for " << get_name() << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

bool NSFSObject::is_expired()
{
  utime_t delete_at;
  if (!decode_attr(state.attrset, RGW_ATTR_DELETE_AT, delete_at)) {
    ldout(driver->ctx(), 0)
        << "ERROR: " << __func__
        << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
    return false;
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

void NSFSObject::gen_rand_obj_instance_name()
{
  state.obj.key.set_instance(gen_rand_instance_name());
}

std::unique_ptr<MPSerializer> NSFSObject::get_serializer(const DoutPrefixProvider *dpp, optional_yield y, const std::string& lock_name)
{
  return std::make_unique<MPNSFSSerializer>(dpp, driver, this, lock_name);
}

int MPNSFSSerializer::try_lock(const DoutPrefixProvider *dpp, ceph::timespan dur, optional_yield y)
{
  if (!obj->check_exists(dpp)) {
    return -ENOENT;
  }

  NSFSBucket* b = static_cast<NSFSBucket*>(obj->get_bucket());
  if (b->get_dir()->get_type() == ObjectType::MULTIPART && b->get_dir_fd(dpp) > 0) {
    locked = true;
    return 0;
  }

  return -ENOENT;
}

int MPNSFSSerializer::unlock(const DoutPrefixProvider *dpp, optional_yield y)
{
  clear_locked();
  return 0;
}

int NSFSObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y,
                            uint32_t flags)
{
  return -ERR_NOT_IMPLEMENTED;
}

int NSFSObject::transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

int NSFSObject::restore_obj_from_cloud(Bucket* bucket,
          rgw::sal::PlacementTier* tier,
	  CephContext* cct,
          std::optional<uint64_t> days,
          bool& in_progress,
	  uint64_t& size,
          const DoutPrefixProvider* dpp,
          optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

bool NSFSObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return (r1 == r2);
}

int NSFSObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
    return 0;
}

int NSFSObject::swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int NSFSObject::swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int NSFSObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  /* TODO Figure out omap */
  return 0;
}

int NSFSObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int NSFSObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  /* TODO Get UID from user */
  int uid = 0;
  int gid = 0;

  int ret = open(dpp, false);
  if (ret < 0) {
    return ret;
  }

  ret = fchown(ent->get_fd(), uid, gid);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not chown object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int NSFSObject::get_cur_version(const DoutPrefixProvider* dpp, rgw_obj_key& key)
{
  return 0;
}

int NSFSObject::stat(const DoutPrefixProvider* dpp)
{
  int ret;

  if (!ent) {
    dir_chain.clear();
    state.is_dm = false;
    dm_version_id.clear();
    Directory* leaf_dir;
    std::string leaf_name;
    ret = resolve_path(dpp,
        static_cast<NSFSBucket *>(bucket)->get_dir(),
        get_fname(/*use_version=*/false),
        /*create_dirs=*/false,
        driver->ctx(),
        dir_chain, leaf_dir, leaf_name);
    if (ret < 0) {
      state.exists = false;
      return ret;
    }

    const std::string& req_ver = state.obj.key.instance;
    NSFSBucket* b = static_cast<NSFSBucket*>(bucket);
    bool versioned = b && b->get_info().versioned();

    if (versioned && !req_ver.empty()) {
      /* version-aware stat: check current first, fall back to .versions/ */
      ret = leaf_dir->get_ent(dpp, null_yield, leaf_name, req_ver, ent);
      if (ret == 0) {
        ret = ent->stat(dpp);
      }

      bool cur_matches = false;
      if (ret == 0 && ent && ent->exists()) {
        if (req_ver == NULL_VERSION_ID) {
          int chk_fd = ent->get_fd();
          if (chk_fd < 0) {
            ent->open(dpp);
            chk_fd = ent->get_fd();
          }
          cur_matches = (chk_fd >= 0 && is_null_version_fd(chk_fd));
        } else {
          std::string cur_ver = nsfs_version_id_from_statx(ent->get_stx());
          cur_matches = (cur_ver == req_ver);
        }
      }

      if (!cur_matches) {
        /* look in .versions/ */
        int parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
        if (parent_fd >= 0) {
          std::string ver_name = nsfs_ver_entry(leaf_name, req_ver);
          int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                             O_RDONLY | O_DIRECTORY);
          if (vfd >= 0) {
            /* create a .versions/ Directory and get the versioned File */
            auto ver_dir = std::make_unique<Directory>(
              HIDDEN_VERSIONS_PATH, leaf_dir, driver->ctx());
            ver_dir->open(dpp);
            ent = std::make_unique<File>(ver_name, ver_dir.get(), driver->ctx());
            dir_chain.push_back(std::move(ver_dir));
            ::close(vfd);

            ret = ent->stat(dpp);
            if (ret < 0) {
              state.exists = false;
              return ret;
            }
          } else {
            state.exists = false;
            return -ENOENT;
          }
        } else {
          state.exists = false;
          return -ENOENT;
        }
      }
    } else {
      /* non-versioned or no specific version requested */
      ret = leaf_dir->get_ent(dpp, null_yield, leaf_name,
          state.obj.key.instance, ent);
      if (ret < 0 && versioned) {
        int parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
        if (parent_fd >= 0) {
          int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                             O_RDONLY | O_DIRECTORY);
          if (vfd >= 0) {
            /* find the newest version entry for this key */
            DIR* vdir = fdopendir(dup(vfd));
            if (vdir) {
              uint64_t max_mtime = 0;
              std::string max_name;
              struct dirent* de;
              while ((de = readdir(vdir)) != nullptr) {
                if (de->d_name[0] == '.') {
                  continue;
                }
                std::string vname(de->d_name);
                /* match entries for this key: "leafname_..." */
                if (vname.size() <= leaf_name.size() + 1 ||
                    vname.compare(0, leaf_name.size(), leaf_name) != 0 ||
                    vname[leaf_name.size()] != '_') {
                  continue;
                }
                std::string vid = vname.substr(leaf_name.size() + 1);
                nsfs_version_info vinfo;
                if (!nsfs_parse_version_id(vid, vinfo)) {
                  continue;
                }
                uint64_t entry_mtime;
                if (vid == NULL_VERSION_ID) {
                  struct statx nstx;
                  if (statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                            STATX_MTIME, &nstx) < 0) {
                    continue;
                  }
                  entry_mtime = statx_mtime_ns(nstx);
                } else {
                  entry_mtime = vinfo.mtime_ns;
                }
                if (entry_mtime > max_mtime || max_name.empty()) {
                  max_mtime = entry_mtime;
                  max_name = vname;
                }
              }
              closedir(vdir);

              if (!max_name.empty()) {
                /* check if it's a delete marker */
                int dm_fd = ::openat(vfd, max_name.c_str(), O_RDONLY);
                if (dm_fd >= 0) {
                  char buf[8];
                  std::string dm_xattr =
                    NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
                  ssize_t xlen = ::fgetxattr(
                    dm_fd, dm_xattr.c_str(), buf, sizeof(buf));
                  if (xlen > 0) {
                    state.is_dm = true;
                    state.exists = false;
                    dm_version_id =
                      max_name.substr(leaf_name.size() + 1);
                    struct statx dm_stx;
                    if (statx(dm_fd, "", AT_EMPTY_PATH,
                              STATX_MTIME, &dm_stx) == 0) {
                      state.mtime =
                        from_statx_timestamp(dm_stx.stx_mtime);
                    }
                  }
                  ::close(dm_fd);
                }
              }
            }
            ::close(vfd);

            if (state.is_dm) {
              state.accounted_size = state.size = 0;
              return -ENOENT;
            }
          }
        }
        state.exists = false;
        return ret;
      } else if (ret < 0) {
        state.exists = false;
        return ret;
      }

      ret = ent->stat(dpp);
      if (ret < 0) {
        state.exists = false;
        return ret;
      }
    }
  } else {
    ret = ent->stat(dpp);
    if (ret < 0) {
      state.exists = false;
      return ret;
    }
  }

  if (state.obj.key.instance.empty()) {
    state.obj.key.instance = ent ? ent->get_cur_version() : "";
  }

  state.exists = ent ? ent->exists() : false;
  if (!state.exists) {
    return 0;
  }

  state.accounted_size = state.size = ent->get_stx().stx_size;
  state.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);

  /* check for delete marker xattr */
  NSFSBucket* bk = static_cast<NSFSBucket*>(bucket);
  if (bk && bk->get_info().versioned() && ent->get_parent()) {
    int pfd = ent->get_parent()->get_fd();
    if (pfd >= 0) {
      int tfd = ::openat(pfd, ent->get_name().c_str(), O_RDONLY);
      if (tfd >= 0) {
        char buf[8];
        std::string dm_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
        ssize_t xlen = ::fgetxattr(tfd, dm_xattr.c_str(), buf, sizeof(buf));
        state.is_dm = (xlen > 0);
        ::close(tfd);
      }
    }
  }

  return 0;
}

int NSFSObject::make_ent(ObjectType type)
{
  if (ent)
    return 0;

  Directory* leaf_dir;
  std::string leaf_name;
  int ret = resolve_path(nullptr,
      static_cast<NSFSBucket *>(bucket)->get_dir(),
      get_fname(/*use_version=*/true),
      /*create_dirs=*/true,
      driver->ctx(),
      dir_chain, leaf_dir, leaf_name);
  if (ret < 0)
    return ret;

  switch (type.type) {
    case ObjectType::UNKNOWN:
      return -EINVAL;
    case ObjectType::FILE:
      ent = std::make_unique<File>(leaf_name, leaf_dir, driver->ctx());
      break;
    case ObjectType::DIRECTORY:
      ent = std::make_unique<Directory>(leaf_name, leaf_dir, driver->ctx());
      break;
    case ObjectType::MULTIPART:
      ent = std::make_unique<MPDirectory>(leaf_name, leaf_dir, driver->ctx());
      break;
  }

  return 0;
}

int NSFSObject::get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner)
{
  ACLOwner acl_owner;
  int ret = decode_acl_owner(get_attrs(), acl_owner);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__
        << ": No " RGW_ATTR_ACL " attr" << dendl;
    return ret;
  }

  if (const auto* u = std::get_if<rgw_user>(&acl_owner.id)) {
    *owner = driver->get_user(*u);
  } else {
    *owner = driver->get_user(rgw_user(std::get<rgw_account_id>(acl_owner.id)));
  }
  (*owner)->load_user(dpp, y);
  return 0;
}

std::unique_ptr<Object::ReadOp> NSFSObject::get_read_op()
{
  return std::make_unique<NSFSReadOp>(this);
}

std::unique_ptr<Object::DeleteOp> NSFSObject::get_delete_op()
{
  return std::make_unique<NSFSDeleteOp>(this);
}

int NSFSObject::open(const DoutPrefixProvider* dpp, bool create, bool temp_file)
{
  int ret{0};

  if (!ent) {
    ret = stat(dpp);
    if (ret < 0) {
      if (!create) {
	return ret;
      }
      ret = make_ent(ObjectType::FILE);
    }
  }
  if (ret < 0) {
    return ret;
  }

  if (create) {
    ret = ent->create(dpp, nullptr, temp_file);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not create " << ent->get_name() << dendl;
      return ret;
    }
  }

  return ent->open(dpp);
}

int NSFSObject::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string temp_fname = gen_temp_fname();
  int ret = ent->link_temp_file(dpp, y, temp_fname);
  if (ret < 0)
    return ret;

  NSFSBucket *b = static_cast<NSFSBucket *>(get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name()
		      << dendl;
    return -EINVAL;
  }

  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: NSFSAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = ent->stat(dpp, /*force=*/true);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: NSFSAtomicWriter failed stat after link" << dendl;
    return ret;
  }

  uint32_t flags = FSEnt::FLAG_NONE;
  const auto& binfo = b->get_info();
  if (binfo.versioned()) {
    int fd = ent->get_fd();
    if (fd >= 0) {
      const struct statx& stx = ent->get_stx();
      std::string ver_id = binfo.versioning_enabled()
        ? nsfs_version_id_from_statx(stx) : NULL_VERSION_ID;
      std::string xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
      ::fsetxattr(fd, xattr.c_str(), ver_id.c_str(), ver_id.size(), 0);
      set_instance(ver_id);
    }
    flags = FSEnt::FLAG_LIST_VERSIONS;
  }

  ent->fill_cache(nullptr, null_yield,
      [&](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &bde) -> int {
	driver->get_bucket_cache()->add_entry(dpp, b->get_name(), bde);
	return 0;
      }, flags);
  return 0;
}


int NSFSObject::close()
{
  if (ent)
    return ent->close();

  return 0;
}

int NSFSObject::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!ent)
    return -ENOENT;
  return ent->read(ofs, left, bl, dpp, y);
}

int NSFSObject::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  return ent->write(ofs, bl, dpp, y);
}

int NSFSObject::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return ent->write_attrs(dpp, y, state.attrset, nullptr);
}

int NSFSObject::NSFSReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = source->stat(dpp);
  if (ret < 0)
    return ret;

  /* set version ID for GET/HEAD response if not already set */
  NSFSBucket* sb = static_cast<NSFSBucket*>(source->get_bucket());
  if (sb && sb->get_info().versioned() &&
      source->get_instance().empty() &&
      source->get_fsent() && source->get_fsent()->exists()) {
    source->set_instance(
      nsfs_version_id_from_statx(source->get_fsent()->get_stx()));
  }

  ret = source->get_obj_attrs(y, dpp);
  if (ret < 0)
    return ret;

  bufferlist etag_bl;
  if (!source->get_attr(RGW_ATTR_ETAG, etag_bl)) {
    /* Sideloaded file.  Generate necessary attributes. Only done once. */
    int ret = source->generate_attrs(dpp, y);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not generate attrs for " << source->get_name() << " error: " << cpp_strerror(ret) << dendl;
	return ret;
    }
  }

  if (!source->get_attr(RGW_ATTR_ETAG, etag_bl)) {
    return -EINVAL;
  }

  {
    uint16_t pc = 0;
    if (decode_raw_attr(source->get_attrs(), RGW_NSFS_ATTR_MULTIPART_PART_COUNT, pc) && pc > 0) {
      params.parts_count = pc;
    }
  }

  if (params.part_num) {
    int pn = *params.part_num;
    std::vector<uint64_t> part_sizes;
    if (!decode_raw_attr(source->get_attrs(), RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes)) {
      if (pn == 1) {
        params.parts_count = 1;
      } else {
        return -ERR_INVALID_PART;
      }
    } else {
      if (pn < 1 || pn > (int)part_sizes.size()) {
        return -ERR_INVALID_PART;
      }
      int64_t ofs = 0;
      for (int i = 0; i < pn - 1; ++i) {
        ofs += part_sizes[i];
      }
      part_ofs = ofs;
      source->set_obj_size(part_sizes[pn - 1]);
    }
  }

#if 0 // WIP
  if (params.mod_ptr || params.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);
    src_weight.high_precision = params.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = params.high_precision_time;

    if (params.mod_ptr && !params.if_nomatch) {
      dest_weight.init(*params.mod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      dest_weight.init(*params.unmod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
#endif

  if (params.mod_ptr || params.unmod_ptr) {
    if (params.mod_ptr && !params.if_nomatch) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.mod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (!(*params.mod_ptr < source->get_mtime())) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.unmod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (*params.unmod_ptr < source->get_mtime()) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (params.if_match) {
    std::string if_match_str = rgw_string_unquote(params.if_match);
    ldpp_dout(dpp, 10) << "If-Match: " << if_match_str << " ETAG: " << etag_bl.c_str() << dendl;

    if (if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (params.if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
    ldpp_dout(dpp, 10) << "If-No-Match: " << if_nomatch_str << " ETAG: " << etag_bl.c_str() << dendl;
    if (if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_NOT_MODIFIED;
    }
  }

  if (params.lastmod) {
    *params.lastmod = source->get_mtime();
  }

  return 0;
}

int NSFSObject::NSFSReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  return source->read(ofs + part_ofs, end + 1, bl, dpp, y);
}

int NSFSObject::generate_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  ret = generate_etag(dpp, y);
  return ret;
}

int NSFSObject::generate_mp_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int NSFSObject::generate_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t left = get_size();
  int64_t cur_ofs = 0;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

  while (left > 0) {
    bufferlist bl;
    int len = read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }
    hash.Update((const unsigned char *)bl.c_str(), bl.length());

    left -= len;
    cur_ofs += len;
  }

  hash.Final(m);
  bufferlist etag_bl;
  append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1, [&](auto iter) {
    iter = buf_to_hex(m, iter);
    *iter++ = '\0';
    return iter;
  });
  get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));
  return write_attrs(dpp, y);
}

const std::string NSFSObject::get_fname(bool use_version)
{
  return get_key_fname(state.obj.key, use_version);
}

std::string NSFSObject::gen_temp_fname()
{
  std::string temp_fname;
  enum { RAND_SUFFIX_SIZE = 8 };
  char buf[RAND_SUFFIX_SIZE + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, RAND_SUFFIX_SIZE);
  std::string key_path = get_fname(/*use_version=*/true);
  auto last_slash = key_path.rfind('/');
  std::string basename = (last_slash != std::string::npos)
    ? key_path.substr(last_slash + 1) : key_path;
  temp_fname = "." + basename + ".";
  temp_fname.append(buf);

  return temp_fname;
}

int NSFSObject::NSFSReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int64_t left;
  int64_t cur_ofs = ofs + part_ofs;
  end += part_ofs;

  if (end < 0)
    left = 0;
  else
    left = end - ofs + 1;

  while (left > 0) {
    bufferlist bl;
    int len = source->read(cur_ofs, left, bl, dpp, y);
    if (len < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not read " << source->get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
	return len;
    } else if (len == 0) {
      /* Done */
      break;
    }

    /* Read some */
    int ret = cb->handle_data(bl, 0, len);
    if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: callback failed on " << source->get_name() << ": " << ret << dendl;
	return ret;
    }

    left -= len;
    cur_ofs += len;
  }

  /* Doesn't seem to be anything needed from params */
  return 0;
}

int NSFSObject::NSFSReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  if (!source->check_exists(dpp)) {
    return -ENOENT;
  }
  if (source->get_obj_attrs(y, dpp) < 0) {
    return -ENODATA;
  }
  if (!source->get_attr(name, dest)) {
    return -ENODATA;
  }

  return 0;
}

/*
 * Check delete preconditions (if_match, last_mod_time_match, size_match)
 * against the given target metadata.  Returns 0 if all pass, or
 * -ERR_PRECONDITION_FAILED on mismatch.
 */
static int check_delete_preconditions(
    const rgw::sal::Object::DeleteOp::Params& params,
    const rgw::sal::Attrs& attrs,
    ceph::real_time mtime,
    uint64_t size)
{
  if (params.if_match && strcmp(params.if_match, "*") != 0) {
    auto it = attrs.find(RGW_ATTR_ETAG);
    if (it == attrs.end()) {
      return -ERR_PRECONDITION_FAILED;
    }
    std::string etag = it->second.to_str();
    std::string if_match_str = rgw_string_unquote(params.if_match);
    if (if_match_str != etag) {
      return -ERR_PRECONDITION_FAILED;
    }
  }

  if (!real_clock::is_zero(params.last_mod_time_match)) {
    if (params.last_mod_time_match_precise) {
      if (params.last_mod_time_match != mtime) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      if (real_clock::to_time_t(params.last_mod_time_match) !=
          real_clock::to_time_t(mtime)) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (params.size_match.has_value()) {
    if (*params.size_match != size) {
      return -ERR_PRECONDITION_FAILED;
    }
  }

  return 0;
}

int NSFSObject::NSFSDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y, uint32_t flags)
{
  bool has_cond = params.if_match ||
    !real_clock::is_zero(params.last_mod_time_match) ||
    params.size_match.has_value();

  NSFSBucket *b = static_cast<NSFSBucket*>(source->get_bucket());
  if (!b) {
    return -EINVAL;
  }

  const auto& binfo = b->get_info();
  bool versioned = binfo.versioned();
  bool ver_enabled = binfo.versioning_enabled();
  /*
   * Single DELETE with ?versionId= → params.marker_version_id.
   * Multi-object DeleteObjects → object key instance.
   * Plain DELETE without versionId → both empty.
   *
   * When load_obj_state detects a delete marker, it copies dm_version_id
   * into instance.  Skip that case so plain DELETE stacks new DMs rather
   * than deleting the existing one.  Multi-delete sets instance from
   * the request (dm_version_id stays empty), so it is not skipped.
   */
  std::string req_version_id = params.marker_version_id;
  if (req_version_id.empty()) {
    std::string inst = source->get_instance();
    if (!inst.empty() && inst != source->dm_version_id) {
      req_version_id = inst;
    }
  }

  if (!versioned) {
    if (has_cond) {
      int ret = source->stat(dpp);
      if (ret == -ENOENT) {
        return 0;
      }
      if (ret < 0) {
        return ret;
      }
      ret = check_delete_preconditions(params, source->get_attrs(),
                                       source->get_mtime(),
                                       source->get_size());
      if (ret < 0) {
        return ret;
      }
    }
    return source->delete_object(dpp, y, flags, nullptr, nullptr);
  }

  /* versioned delete with specific versionId */
  if (!req_version_id.empty()) {
    ldpp_dout(dpp, 10) << "delete_obj: version-specific vid="
      << req_version_id << " key=" << source->get_name() << dendl;

    /* resolve path to get parent dir for version lock;
     * stat without instance so we always get the object's parent
     * directory, not .versions/ */
    std::string saved_instance = source->get_instance();
    source->clear_instance();
    source->ent.reset();
    int ret = source->stat(dpp);
    nsfs::FSEnt* ent = source->get_fsent();

    /* acquire version lock — must use the object's parent dir,
     * not .versions/, so all competing threads contend on the
     * same lock file */
    int lock_parent_fd = -1;
    if (ent && ent->get_parent()) {
      lock_parent_fd = ent->get_parent()->get_fd();
      if (lock_parent_fd < 0) {
        ent->get_parent()->open(dpp);
        lock_parent_fd = ent->get_parent()->get_fd();
      }
    } else {
      /* object doesn't exist as current — resolve path just for lock */
      std::vector<std::unique_ptr<nsfs::Directory>> lk_chain;
      nsfs::Directory* lk_dir = nullptr;
      std::string lk_leaf;
      int r = nsfs::resolve_path(dpp, b->get_dir(),
          source->get_fname(/*use_version=*/false),
          /*create_dirs=*/false, source->driver->ctx(),
          lk_chain, lk_dir, lk_leaf);
      if (r == 0 && lk_dir) {
        lock_parent_fd = lk_dir->get_fd();
        if (lock_parent_fd < 0) {
          lk_dir->open(dpp);
          lock_parent_fd = lk_dir->get_fd();
        }
      }
    }
    auto vlock = (lock_parent_fd >= 0)
      ? source->driver->get_fs_strategy()->version_lock(
          dpp, open_versions_lockfile(lock_parent_fd))
      : std::unique_ptr<VersionLockHandle>();

    /* re-stat under lock with original instance to get fresh state */
    source->set_instance(saved_instance);
    source->ent.reset();
    ret = source->stat(dpp);
    ent = source->get_fsent();

    /* check if the requested version is the current */
    if (ret == 0 && ent) {
      bool cur_match;
      if (req_version_id == NULL_VERSION_ID) {
        int chk_fd = ent->get_fd();
        if (chk_fd < 0) {
          ent->open(dpp);
          chk_fd = ent->get_fd();
        }
        cur_match = (chk_fd >= 0 && is_null_version_fd(chk_fd));
      } else {
        std::string cur_ver_id = nsfs_version_id_from_statx(ent->get_stx());
        cur_match = (req_version_id == cur_ver_id);
        ldpp_dout(dpp, 10) << "delete_obj: cur_ver_id=" << cur_ver_id
          << " cur_match=" << cur_match << dendl;
      }
      if (cur_match) {
        if (has_cond) {
          int r = check_delete_preconditions(
            params, source->get_attrs(),
            source->get_mtime(), source->get_size());
          if (r < 0) {
            return r;
          }
        }
        ldpp_dout(dpp, 10) << "delete_obj: deleting current version "
          << req_version_id << dendl;
        result.version_id = req_version_id;
        /* capture parent fd and leaf name before delete_object
         * invalidates the ent/dir_chain */
        int promote_fd = -1;
        std::string promote_leaf;
        nsfs::Directory* parent = ent->get_parent();
        if (parent) {
          int pfd = parent->get_fd();
          if (pfd < 0) {
            parent->open(dpp);
            pfd = parent->get_fd();
          }
          if (pfd >= 0) {
            promote_fd = ::dup(pfd);
          }
          promote_leaf = ent->get_name();
        }
        ret = source->delete_object(dpp, y, flags, nullptr, nullptr);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "delete_obj: delete_object failed ret="
            << ret << dendl;
          if (promote_fd >= 0) { ::close(promote_fd); }
          return ret;
        }
        if (promote_fd >= 0) {
          ldpp_dout(dpp, 10) << "delete_obj: promoting after delete of "
            << promote_leaf << dendl;
          promote_version(promote_fd, promote_leaf, dpp,
                          source->driver->get_fs_strategy());
          struct statx pstx;
          if (statx(promote_fd, promote_leaf.c_str(),
                    AT_SYMLINK_NOFOLLOW, STATX_ALL, &pstx) == 0) {
            bool promoted_is_null = false;
            {
              int chk = ::openat(promote_fd, promote_leaf.c_str(), O_RDONLY);
              if (chk >= 0) {
                promoted_is_null = is_null_version_fd(chk);
                ::close(chk);
              }
            }
            std::string promoted_ver = promoted_is_null
              ? NULL_VERSION_ID
              : nsfs_version_id_from_statx(pstx);
            rgw_bucket_dir_entry bde{};
            bde.key.name = source->get_key().get_index_key_name();
            bde.key.instance = promoted_ver;
            bde.ver.pool = 1;
            bde.ver.epoch = 1;
            bde.exists = true;
            bde.meta.category = RGWObjCategory::Main;
            bde.meta.size = pstx.stx_size;
            bde.meta.accounted_size = pstx.stx_size;
            bde.meta.mtime = from_statx_timestamp(pstx.stx_mtime);
            bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
            bde.meta.etag = synthesize_etag(pstx);
            bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                        rgw_bucket_dir_entry::FLAG_CURRENT;
            source->driver->get_bucket_cache()->add_entry(
              dpp, b->get_name(), bde);
          }
          ::close(promote_fd);
        }
        return 0;
      }
    }

    /* look in .versions/ — need parent dir even if current is gone */
    int parent_fd = -1;
    std::string leaf_name;
    if (ent && ent->get_parent()) {
      nsfs::Directory* parent = ent->get_parent();
      parent_fd = parent->get_fd();
      if (parent_fd < 0) {
        parent->open(dpp);
        parent_fd = parent->get_fd();
      }
      leaf_name = ent->get_name();
    } else {
      /* current doesn't exist, resolve path to get parent dir */
      std::vector<std::unique_ptr<nsfs::Directory>> chain;
      nsfs::Directory* leaf_dir = nullptr;
      ret = nsfs::resolve_path(dpp, b->get_dir(),
          source->get_fname(/*use_version=*/false),
          /*create_dirs=*/false, source->driver->ctx(),
          chain, leaf_dir, leaf_name);
      if (ret == 0 && leaf_dir) {
        parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
      }
    }

    if (parent_fd >= 0) {
      int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                         O_RDONLY | O_DIRECTORY);
      if (vfd >= 0) {
        std::string ver_name = nsfs_ver_entry(leaf_name, req_version_id);
        bool is_dm = false;
        int ver_fd = ::openat(vfd, ver_name.c_str(), O_RDONLY);
        if (ver_fd < 0) {
          ldpp_dout(dpp, 10) << "delete_obj: " << ver_name
            << " not found in .versions/ (already deleted?)" << dendl;
          ::close(vfd);
          result.version_id = req_version_id;
          return 0;
        }
        ldpp_dout(dpp, 10) << "delete_obj: unlinking " << ver_name
          << " from .versions/" << dendl;

        /* read delete-marker flag */
        {
          char buf[8];
          std::string dm_xattr =
            NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
          ssize_t xlen = ::fgetxattr(ver_fd, dm_xattr.c_str(),
                                     buf, sizeof(buf));
          if (xlen > 0) {
            is_dm = true;
          }
        }

        /* conditional check against version's metadata */
        if (has_cond) {
          rgw::sal::Attrs ver_attrs;
          struct statx ver_stx;
          ceph::real_time ver_mtime;
          uint64_t ver_size = 0;

          if (statx(ver_fd, "", AT_EMPTY_PATH,
                    STATX_MTIME | STATX_SIZE, &ver_stx) == 0) {
            ver_mtime = from_statx_timestamp(ver_stx.stx_mtime);
            ver_size = ver_stx.stx_size;
          }

          /* read etag xattr if not a delete marker */
          if (!is_dm) {
            char ebuf[256];
            std::string etag_x = make_xattr_name(RGW_ATTR_ETAG);
            ssize_t elen = ::fgetxattr(ver_fd, etag_x.c_str(),
                                       ebuf, sizeof(ebuf));
            if (elen > 0) {
              bufferlist bl;
              bl.append(ebuf, elen);
              ver_attrs[RGW_ATTR_ETAG] = std::move(bl);
            }
          }

          int r = check_delete_preconditions(
            params, ver_attrs, ver_mtime, ver_size);
          if (r < 0) {
            ::close(ver_fd);
            ::close(vfd);
            return r;
          }
        }
        ::close(ver_fd);

        ::unlinkat(vfd, ver_name.c_str(), 0);
        ::close(vfd);
        result.version_id = req_version_id;
        result.delete_marker = is_dm;
        {
          cls_rgw_obj_key cache_key;
          cache_key.name = source->get_key().get_index_key_name();
          cache_key.instance = req_version_id;
          source->driver->get_bucket_cache()->remove_entry(
            dpp, b->get_name(), cache_key);
        }
        return 0;
      }
    }
    result.version_id = req_version_id;
    return 0;
  }

  /* versioned delete without versionId — create delete marker */
  {
  /* clear instance and ent so stat() re-resolves from scratch
   * (instance may have been set by load_obj_state's dm detection,
   * and ent may point to a .versions/ entry from a prior stat) */
  source->clear_instance();
  source->ent.reset();
  int ret = source->stat(dpp);

  if (has_cond) {
    if (ret == -ENOENT && source->dm_version_id.empty()) {
      return 0;
    }
    if (ret == -ENOENT) {
      /*
       * DM is current.  stat() set state.mtime and state.size = 0.
       * DMs have no etag, so any specific if_match fails; "*" passes
       * via the helper (no RGW_ATTR_ETAG in empty attrs → skip).
       */
      rgw::sal::Attrs dm_attrs;
      int r = check_delete_preconditions(
        params, dm_attrs, source->get_mtime(), 0);
      if (r < 0) {
        return r;
      }
    } else if (ret < 0) {
      return ret;
    } else {
      int r = check_delete_preconditions(
        params, source->get_attrs(),
        source->get_mtime(), source->get_size());
      if (r < 0) {
        return r;
      }
    }
  }

  nsfs::FSEnt* ent = source->get_fsent();
  int parent_fd = -1;
  std::string dm_leaf;

  if (ent && ent->get_parent()) {
    nsfs::Directory* parent = ent->get_parent();
    parent_fd = parent->get_fd();
    if (parent_fd < 0) {
      parent->open(dpp);
      parent_fd = parent->get_fd();
    }
    dm_leaf = ent->get_name();
  }

  std::vector<std::unique_ptr<nsfs::Directory>> dm_chain;
  if (parent_fd < 0) {
    /* current doesn't exist — resolve path just to get the parent dir */
    nsfs::Directory* leaf_dir = nullptr;
    int r = nsfs::resolve_path(dpp, b->get_dir(),
        source->get_fname(/*use_version=*/false),
        /*create_dirs=*/true, source->driver->ctx(),
        dm_chain, leaf_dir, dm_leaf);
    if (r == 0 && leaf_dir) {
      parent_fd = leaf_dir->get_fd();
      if (parent_fd < 0) {
        leaf_dir->open(dpp);
        parent_fd = leaf_dir->get_fd();
      }
    }
  }

  if (parent_fd < 0) {
    return -EINVAL;
  }

  int vfd = open_versions_dir(parent_fd);
  if (vfd < 0) {
    return vfd;
  }

  auto vlock = source->driver->get_fs_strategy()->version_lock(
    dpp, open_versions_lockfile(parent_fd));

  /* determine if current is a null version */
  bool cur_is_null = false;
  if (ret == 0 && ent && ent->exists()) {
    int chk_fd = ent->get_fd();
    if (chk_fd < 0) {
      ent->open(dpp);
      chk_fd = ent->get_fd();
    }
    if (chk_fd >= 0) {
      cur_is_null = is_null_version_fd(chk_fd);
    }
  }

  /* in suspended mode, remove any existing _null from .versions/ */
  if (!ver_enabled) {
    std::string null_name = nsfs_ver_entry(dm_leaf, NULL_VERSION_ID);
    ::unlinkat(vfd, null_name.c_str(), 0);
  }

  std::string demoted_ver_id;
  bool did_demote = false;

  /* demote current to .versions/ with retry on CAS mismatch;
   * skip when suspended and current is null (S3: replace null version) */
  if (ret == 0 && ent && ent->exists() &&
      !(!ver_enabled && cur_is_null)) {
    for (int attempt = 0; attempt < NSFS_VERSION_RETRIES; ++attempt) {
      struct statx cur_stx;
      if (statx(parent_fd, dm_leaf.c_str(), AT_SYMLINK_NOFOLLOW,
                STATX_ALL, &cur_stx) < 0) {
        break;
      }
      std::string cur_ver_id = cur_is_null
        ? NULL_VERSION_ID
        : nsfs_version_id_from_statx(cur_stx);
      std::string ver_name = nsfs_ver_entry(dm_leaf, cur_ver_id);
      uint64_t cur_mtime = statx_mtime_ns(cur_stx);
      uint64_t cur_ino = cur_stx.stx_ino;

      SafeResult sr = source->driver->get_fs_strategy()->safe_link(
                                dpp, parent_fd, dm_leaf,
                                vfd, ver_name,
                                cur_mtime, cur_ino);
      if (sr == SafeResult::OK) {
        int demoted_fd = ::openat(vfd, ver_name.c_str(), O_RDONLY);
        if (demoted_fd >= 0) {
          auto now_ms = std::to_string(
            std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count());
          std::string ts_x = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_NON_CURRENT_TS;
          ::fsetxattr(demoted_fd, ts_x.c_str(),
                      now_ms.c_str(), now_ms.size(), 0);
          ::close(demoted_fd);
        }
        /* CAS unlink: remove current only if it still matches */
        source->driver->get_fs_strategy()->safe_unlink(
                    dpp, parent_fd, dm_leaf, vfd,
                    cur_mtime, cur_ino);
        demoted_ver_id = cur_ver_id;
        did_demote = true;
        break;
      }
      if (sr == SafeResult::ERROR) {
        break;
      }
      if (sr == SafeResult::MISMATCH && cur_is_null) {
        struct statx chk;
        if (statx(vfd, ver_name.c_str(), AT_SYMLINK_NOFOLLOW,
                  STATX_INO, &chk) == 0) {
          break;
        }
      }
      struct timespec ts = {0, 100000 + (rand() % 500000)};
      nanosleep(&ts, nullptr);
    }
  } else if (!ver_enabled && ret == 0 && ent && ent->exists() && cur_is_null) {
    /* suspended + null current: just unlink (rename will fail if no current) */
    ::unlinkat(parent_fd, dm_leaf.c_str(), 0);
  }

  /* create delete marker as zero-byte file in .versions/ */
  static std::atomic<uint64_t> dm_counter{0};
  std::string dm_tmp = ".dm_tmp_" +
    std::to_string(getpid()) + "_" + std::to_string(dm_counter.fetch_add(1));

  int dm_fd = ::openat(vfd, dm_tmp.c_str(),
                       O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (dm_fd >= 0) {
    /* set delete_marker xattr */
    std::string dm_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_DELETE_MARKER;
    std::string dm_val = "true";
    ::fsetxattr(dm_fd, dm_xattr.c_str(), dm_val.c_str(), dm_val.size(), 0);

    /* stat for version ID */
    struct statx dm_stx;
    std::string dm_ver_id;
    if (statx(dm_fd, "", AT_EMPTY_PATH, STATX_ALL, &dm_stx) == 0) {
      dm_ver_id = ver_enabled
        ? nsfs_version_id_from_statx(dm_stx)
        : NULL_VERSION_ID;
    } else {
      dm_ver_id = NULL_VERSION_ID;
    }

    /* set version_id xattr */
    std::string vid_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
    ::fsetxattr(dm_fd, vid_xattr.c_str(),
                dm_ver_id.c_str(), dm_ver_id.size(), 0);
    ::close(dm_fd);

    /* rename to final name */
    std::string dm_name = nsfs_ver_entry(dm_leaf, dm_ver_id);
    ::renameat(vfd, dm_tmp.c_str(), vfd, dm_name.c_str());

    result.delete_marker = true;
    result.version_id = dm_ver_id;
  }

  ::close(vfd);

  /* surgical cache update: demoted current → non-current, add delete marker */
  auto* bcache = source->driver->get_bucket_cache();
  std::string obj_name = source->get_key().get_index_key_name();

  if (did_demote) {
    /* remove old current entry, add non-current entry */
    cls_rgw_obj_key old_key;
    old_key.name = obj_name;
    old_key.instance = demoted_ver_id;
    bcache->remove_entry(dpp, b->get_name(), old_key);

    rgw_bucket_dir_entry dem_bde{};
    dem_bde.key.name = obj_name;
    dem_bde.key.instance = demoted_ver_id;
    dem_bde.ver.pool = 1;
    dem_bde.ver.epoch = 1;
    dem_bde.exists = true;
    dem_bde.meta.category = RGWObjCategory::Main;
    dem_bde.meta.size = ent->get_stx().stx_size;
    dem_bde.meta.accounted_size = ent->get_stx().stx_size;
    dem_bde.meta.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);
    dem_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    dem_bde.meta.etag = synthesize_etag(ent->get_stx());
    dem_bde.flags = rgw_bucket_dir_entry::FLAG_VER;
    bcache->add_entry(dpp, b->get_name(), dem_bde);
  }

  if (!result.version_id.empty()) {
    /* add delete marker entry */
    rgw_bucket_dir_entry dm_bde{};
    dm_bde.key.name = obj_name;
    dm_bde.key.instance = result.version_id;
    dm_bde.ver.pool = 1;
    dm_bde.ver.epoch = 1;
    dm_bde.exists = true;
    dm_bde.meta.category = RGWObjCategory::Main;
    dm_bde.meta.size = 0;
    dm_bde.meta.accounted_size = 0;
    dm_bde.meta.mtime = ceph::real_clock::now();
    dm_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    dm_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                   rgw_bucket_dir_entry::FLAG_CURRENT |
                   rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
    bcache->add_entry(dpp, b->get_name(), dm_bde);
  }

  return 0;
  } /* versioned delete without versionId */
}

int NSFSObject::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      NSFSBucket *sb, NSFSBucket *db, NSFSObject *dobj)
{
  rgw_obj_key dst_key = dobj->get_key();

  std::vector<std::unique_ptr<Directory>> dst_chain;
  Directory* dst_leaf_dir;
  std::string dst_leaf_name;
  int ret = resolve_path(dpp, db->get_dir(),
      get_key_fname(dst_key, /*use_version=*/false),
      /*create_dirs=*/true, driver->ctx(),
      dst_chain, dst_leaf_dir, dst_leaf_name);
  if (ret < 0)
    return ret;

  return ent->copy(dpp, y, dst_leaf_dir, dst_leaf_name);
}

void NSFSMPObj::init_gen(NSFSDriver* driver, const std::string& _oid, ACLOwner& _owner)
{
  char buf[33];
  std::string new_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
  /* Generate an upload ID */

  gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
  new_id.append(buf);
  init(_oid, new_id, _owner);
}

int NSFSMultipartPart::load(const DoutPrefixProvider* dpp, optional_yield y,
			     NSFSDriver* driver, rgw_obj_key& key)
{
  if (part_file) {
    /* Already loaded */
    return 0;
  }

  part_file = std::make_unique<File>(get_key_fname(key, false), upload->get_shadow()->get_dir(), driver->ctx());

  // Stat the part_file object to get things like size
  int ret = part_file->stat(dpp, y);
  if (ret < 0) {
    return ret;
  }

  Attrs attrs;
  ret = part_file->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  ret = decode_attr(attrs, RGW_NSFS_ATTR_MPUPLOAD, info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": failed to decode part info: " << key << dendl;
    return ret;
  }

  return 0;
}

int NSFSMultipartUpload::load(const DoutPrefixProvider *dpp, bool create)
{
  int ret = 0;
  if (!shadow) {
    NSFSBucket* pb = static_cast<NSFSBucket*>(bucket);
    std::optional<std::string> ns{mp_ns};

    std::string staging_name = get_fname();
    std::unique_ptr<Directory> mpdir = std::make_unique<MPDirectory>(staging_name, pb->get_dir(), driver->ctx());

    shadow = std::make_unique<NSFSBucket>(driver, std::move(mpdir), rgw_bucket(std::string(), mp_obj.upload_id), mp_ns);

    ret = shadow->load_bucket(dpp, null_yield);
    if (ret == -ENOENT && create) {
      ret = shadow->create(dpp, null_yield, nullptr);
    }
  }

  return ret;
}

std::unique_ptr<rgw::sal::Object> NSFSMultipartUpload::get_meta_obj()
{
  std::unique_ptr<rgw::sal::Object> meta_obj{nullptr};

  load(nullptr);

  static const std::string meta_name{".meta"};
  if (!shadow) {
    meta_obj = bucket->get_object(rgw_obj_key(get_meta(), std::string(), mp_ns));
  } else {
    meta_obj = shadow->get_object(rgw_obj_key(meta_name, std::string()));
  }

  auto nsfs_meta_obj = static_cast<NSFSObject*>(meta_obj.get());
  if (shadow) {
    nsfs_meta_obj->pin_bucket(shadow->clone());
  }
  rgw::sal::Attrs attrs;
  if (obj_retention) {
    buffer::list obj_retention_bl;
    obj_retention->encode(obj_retention_bl);
    attrs[RGW_ATTR_OBJECT_RETENTION] = std::move(obj_retention_bl);
  }
  if (obj_legal_hold) {
    buffer::list obj_legal_hold_bl;
    obj_legal_hold->encode(obj_legal_hold_bl);
    attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = std::move(obj_legal_hold_bl);
  }
  nsfs_meta_obj->set_attrs(attrs);

  return meta_obj;
}

int NSFSMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  int ret;

  /* Create the shadow bucket */
  ret = load(dpp, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << " ERROR: could not get shadow dir for mp upload "
      << get_key() << dendl;
    return ret;
  }

  /* Now create the meta object */
  std::unique_ptr<rgw::sal::Object> meta_obj;

  meta_obj = get_meta_obj();

  ret = static_cast<NSFSObject*>(meta_obj.get())->open(dpp, true);
  if (ret < 0) {
    return ret;
  }

  mp_obj.upload_info.cksum_type = cksum_type;
  mp_obj.upload_info.cksum_flags = cksum_flags;

  if (obj_retention) {
    mp_obj.upload_info.obj_retention_exist = true;
    mp_obj.upload_info.obj_retention = *obj_retention;
  }
  if (obj_legal_hold) {
    mp_obj.upload_info.obj_legal_hold_exist = true;
    mp_obj.upload_info.obj_legal_hold = *obj_legal_hold;
  }

  mp_obj.upload_info.dest_placement = dest_placement;
  mp_obj.owner = owner;

  bufferlist bl;
  encode(mp_obj, bl);

  attrs[RGW_NSFS_ATTR_MPUPLOAD] = bl;

  return meta_obj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
}

int NSFSMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated, optional_yield y,
				      bool assume_unsorted)
{
  int ret;
  int last_num = 0;

  ret = load(dpp);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = MP_OBJ_PART_PFX;
  params.marker = MP_OBJ_PART_PFX + fmt::format("{:0>5}", marker);
  params.marker.ns = mp_ns;
  params.ns = mp_ns;

  ret = shadow->list(dpp, params, num_parts + 1, results, y);
  if (ret < 0) {
    return ret;
  }
  for (rgw_bucket_dir_entry& ent : results.objs) {
    std::unique_ptr<MultipartPart> part = std::make_unique<NSFSMultipartPart>(this);
    NSFSMultipartPart* ppart = static_cast<NSFSMultipartPart*>(part.get());

    rgw_obj_key key(ent.key);
    // Parts are namespaced in the bucket listing
    key.ns.clear();
    ret = ppart->load(dpp, y, driver, key);
    if (ret == 0) {
      /* Skip anything that's not a part */
      last_num = part->get_num();
      parts[part->get_num()] = std::move(part);
    }
    if (parts.size() == (ulong)num_parts)
      break;
  }

  if (truncated)
    *truncated = results.is_truncated;

  if (next_marker)
    *next_marker = last_num;

  return 0;
}

int NSFSMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
{
  int ret;

  ret = load(dpp);
  if (ret < 0) {
    if (ret == -ENOENT)
      ret = ERR_NO_SUCH_UPLOAD;
    return ret;
  }

  driver->get_bucket_cache()->invalidate_bucket(dpp, shadow->get_name(), true);
  shadow->remove(dpp, true, y);

  return 0;
}

int NSFSMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
				    prefix_map_t& processed_prefixes,
            const char *if_match,
            const char *if_nomatch)
{
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs& attrs = target_obj->get_attrs();
  std::vector<uint64_t> part_sizes;

  ofs = accounted_size = 0;

  do {
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated, y);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret < 0)
      return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }

    for (auto obj_iter = parts.begin(); etags_iter != part_etags.end() && obj_iter != parts.end(); ++etags_iter, ++obj_iter, ++handled_parts) {
      NSFSMultipartPart* part = static_cast<rgw::sal::NSFSMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
			 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      std::string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      hex_to_buf(part->get_etag().c_str(), petag,
		CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));

      // Compression is not supported yet
#if 0
      RGWUploadPartInfo& obj_part = part->info;

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
      }

      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }
#endif

      part_sizes.push_back(part->get_size());
      ofs += part->get_size();
      accounted_size += part->get_size();
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  bufferlist etag_bl;
  append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16, [&](auto iter) {
    iter = buf_to_hex(final_etag, iter);
    iter = fmt::format_to(iter, "-{}", part_etags.size());
    return iter;
  });

  attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  {

    uint16_t pc = total_parts;
    encode_attr(attrs, RGW_NSFS_ATTR_MULTIPART_PART_COUNT, pc);
    encode_attr(attrs, RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes);
  }

  NSFSBucket* pb = static_cast<NSFSBucket*>(bucket);
  int staging_fd = shadow->get_dir()->get_fd();

  // assemble parts into a single file via copy_file_range
  std::string assembled_name = ".assembled";
  ret = assemble_parts(dpp, staging_fd, total_parts, assembled_name);
  if (ret < 0) {
    return ret;
  }

  // write xattrs on assembled file
  int afd = openat(staging_fd, assembled_name.c_str(), O_RDWR);
  if (afd < 0) {
    return -errno;
  }

  /* owner is already in attrs[RGW_ATTR_ACL] from the generic layer */

  // encode object type as FILE
  ObjectType file_type;
  file_type.type = ObjectType::FILE;
  bufferlist type_bl;
  file_type.encode(type_bl);
  attrs[RGW_NSFS_ATTR_OBJECT_TYPE] = std::move(type_bl);

  for (auto& [k, v] : attrs) {
    std::string xattr_name = make_xattr_name(k);
    ret = fsetxattr(afd, xattr_name.c_str(), v.c_str(), v.length(), 0);
    if (ret < 0) {
      ::close(afd);
      return -errno;
    }
  }
  ::close(afd);

  // resolve hierarchical target path
  std::string target_key = target_obj->get_name();
  std::vector<std::unique_ptr<Directory>> dir_chain;
  Directory* leaf_dir;
  std::string leaf_name;
  ret = resolve_path(dpp, pb->get_dir(), target_key,
                           /*create_dirs=*/true, driver->ctx(),
                           dir_chain, leaf_dir, leaf_name);
  if (ret < 0) {
    return ret;
  }

  bool versioned = pb->get_info().versioned();
  bool ver_enabled = pb->get_info().versioning_enabled();
  int leaf_fd = leaf_dir->get_fd();

  /* conditional write checks against existing target object */
  if (if_match || if_nomatch) {
    int existing_fd = ::openat(leaf_fd, leaf_name.c_str(), O_RDONLY);
    bool target_exists = (existing_fd >= 0);
    std::string existing_etag;
    if (target_exists) {
      char buf[256];
      std::string xattr_name = make_xattr_name(RGW_ATTR_ETAG);
      ssize_t len = ::fgetxattr(existing_fd, xattr_name.c_str(),
                                buf, sizeof(buf));
      if (len > 0) {
        existing_etag.assign(buf, len);
      }
      ::close(existing_fd);
    }

    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        if (!target_exists) {
          return -ENOENT;
        }
      } else {
        if (!target_exists) {
          return -ENOENT;
        }
        std::string if_match_str = rgw_string_unquote(if_match);
        if (if_match_str != existing_etag) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        if (target_exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else if (target_exists && !existing_etag.empty()) {
        std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
        if (if_nomatch_str == existing_etag) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  bool did_demote = false;
  std::string demoted_ver_id;

  /* versioned: demote current version before publishing, with lock */
  if (versioned && leaf_fd >= 0) {
    int vfd = open_versions_dir(leaf_fd);
    if (vfd >= 0) {
      auto vlock = driver->get_fs_strategy()->version_lock(
        dpp, open_versions_lockfile(leaf_fd));

      if (!ver_enabled) {
        std::string null_name = nsfs_ver_entry(leaf_name, NULL_VERSION_ID);
        ::unlinkat(vfd, null_name.c_str(), 0);
      }

      struct statx cur_stx;
      if (statx(leaf_fd, leaf_name.c_str(), AT_SYMLINK_NOFOLLOW,
                STATX_ALL, &cur_stx) == 0) {
        bool cur_is_null = false;
        {
          int chk_fd = ::openat(leaf_fd, leaf_name.c_str(), O_RDONLY);
          if (chk_fd >= 0) {
            cur_is_null = is_null_version_fd(chk_fd);
            ::close(chk_fd);
          }
        }
        if (!(!ver_enabled && cur_is_null)) {
          std::string cur_ver_id = cur_is_null
            ? NULL_VERSION_ID
            : nsfs_version_id_from_statx(cur_stx);
          std::string ver_name = nsfs_ver_entry(leaf_name, cur_ver_id);

          SafeResult sr = driver->get_fs_strategy()->safe_link(
                                    dpp, leaf_fd, leaf_name, vfd, ver_name,
                                    statx_mtime_ns(cur_stx), cur_stx.stx_ino);
          if (sr == SafeResult::OK) {
            int demoted_fd = ::openat(vfd, ver_name.c_str(), O_RDONLY);
            if (demoted_fd >= 0) {
              auto now_ms = std::to_string(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count());
              std::string ts_x =
                NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_NON_CURRENT_TS;
              ::fsetxattr(demoted_fd, ts_x.c_str(),
                          now_ms.c_str(), now_ms.size(), 0);
              ::close(demoted_fd);
            }
            demoted_ver_id = cur_ver_id;
            did_demote = true;
          } else if (sr == SafeResult::MISMATCH && cur_is_null) {
            /* null version already demoted */
          } else if (sr != SafeResult::OK) {
            ldpp_dout(dpp, 0) << "ERROR: versioned MPU demote failed for "
              << leaf_name << " sr=" << (int)sr << dendl;
            ::close(vfd);
            return -ERR_INTERNAL_ERROR;
          }
        }
      }

      ret = renameat(staging_fd, assembled_name.c_str(),
                     leaf_fd, leaf_name.c_str());
      ::close(vfd);
    } else {
      ret = renameat(staging_fd, assembled_name.c_str(),
                     leaf_fd, leaf_name.c_str());
    }
  } else {
    ret = renameat(staging_fd, assembled_name.c_str(),
                   leaf_fd, leaf_name.c_str());
  }

  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: failed to rename assembled file to "
                      << target_key << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  /* versioned: compute version ID from published file's stat */
  if (versioned && leaf_fd >= 0) {
    struct statx new_stx;
    if (statx(leaf_fd, leaf_name.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_ALL, &new_stx) == 0) {
      std::string new_ver_id = ver_enabled
        ? nsfs_version_id_from_statx(new_stx) : NULL_VERSION_ID;
      target_obj->set_instance(new_ver_id);
      int obj_fd = ::openat(leaf_fd, leaf_name.c_str(), O_RDONLY);
      if (obj_fd >= 0) {
        std::string vid_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
        ::fsetxattr(obj_fd, vid_xattr.c_str(),
                    new_ver_id.c_str(), new_ver_id.size(), 0);
        ::close(obj_fd);
      }

      auto* bcache = driver->get_bucket_cache();
      std::string obj_name = target_obj->get_key().get_index_key_name();

      if (did_demote) {
        cls_rgw_obj_key old_key;
        old_key.name = obj_name;
        old_key.instance = demoted_ver_id;
        bcache->remove_entry(dpp, pb->get_name(), old_key);

        rgw_bucket_dir_entry dem_bde{};
        dem_bde.key.name = obj_name;
        dem_bde.key.instance = demoted_ver_id;
        dem_bde.ver.pool = 1;
        dem_bde.ver.epoch = 1;
        dem_bde.exists = true;
        dem_bde.meta.category = RGWObjCategory::Main;
        dem_bde.flags = rgw_bucket_dir_entry::FLAG_VER;
        bcache->add_entry(dpp, pb->get_name(), dem_bde);
      }

      rgw_bucket_dir_entry new_bde{};
      new_bde.key.name = obj_name;
      new_bde.key.instance = new_ver_id;
      new_bde.ver.pool = 1;
      new_bde.ver.epoch = 1;
      new_bde.exists = true;
      new_bde.meta.category = RGWObjCategory::Main;
      new_bde.meta.size = new_stx.stx_size;
      new_bde.meta.accounted_size = new_stx.stx_size;
      new_bde.meta.mtime = from_statx_timestamp(new_stx.stx_mtime);
      new_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
      new_bde.meta.etag = synthesize_etag(new_stx);
      new_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                      rgw_bucket_dir_entry::FLAG_CURRENT;
      bcache->add_entry(dpp, pb->get_name(), new_bde);
    }
  }

  // remove staging directory and its listing cache entry
  driver->get_bucket_cache()->invalidate_bucket(dpp, shadow->get_name(), true);
  shadow->get_dir()->close();
  delete_directory(pb->get_dir()->get_fd(),
                   get_fname().c_str(), true, dpp);

  // update bucket cache
  target_obj->set_obj_size(ofs);

  return 0;
}

int NSFSMultipartUpload::cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
    CephContext *cct, optional_yield y,
    const rgw_obj& obj,
    std::list<rgw_obj_index_key>& remove_objs,
    prefix_map_t& processed_prefixes)
{
  return -ENOTSUP;
}

int NSFSMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y,
				   rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
{
  std::unique_ptr<rgw::sal::Object> meta_obj;
  int ret;

  if (!rule && !attrs) {
    return 0;
  }

  if (attrs) {
      meta_obj = get_meta_obj();
      int ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
	  << get_key() << dendl;
	return ret;
      }
      *attrs = meta_obj->get_attrs();
  }

  if (rule) {
    if (mp_obj.upload_info.dest_placement.name.empty()) {
      if (!meta_obj) {
	meta_obj = get_meta_obj();
      }
      ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
                          << get_key() << dendl;
        return ret;
      }
      ret = decode_attr(meta_obj->get_attrs(), RGW_NSFS_ATTR_MPUPLOAD, mp_obj);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object attrs for mp upload "
	  << get_key() << dendl;
	return ret;
      }
    }
    *rule = &mp_obj.upload_info.dest_placement;

    if (mp_obj.upload_info.obj_retention_exist) {
      obj_retention = mp_obj.upload_info.obj_retention;
    }
    if (mp_obj.upload_info.obj_legal_hold_exist) {
      obj_legal_hold = mp_obj.upload_info.obj_legal_hold;
    }

    /* no te olvides los cksum */
    cksum_type = mp_obj.upload_info.cksum_type;
    cksum_flags = mp_obj.upload_info.cksum_flags;
  }

  return 0;
}

std::string NSFSMultipartUpload::get_fname()
{
  return "." + mp_ns + "_" + mp_obj.upload_id;
}

std::unique_ptr<Writer> NSFSMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  std::string fname = MP_OBJ_PART_PFX + fmt::format("{:0>5}", part_num);
  rgw_obj_key part_key(fname);

  load(dpp);

  return std::make_unique<NSFSMultipartWriter>(dpp, y, shadow.get(), part_key,
                                                driver, owner,
                                                ptail_placement_rule, part_num);
}

int NSFSMultipartWriter::prepare(optional_yield y)
{
  int ret = part_file->create(dpp, /*existed=*/nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }

  return part_file->open(dpp);
}

int NSFSMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  return part_file->write(offset, data, dpp, null_yield);
}

int NSFSMultipartWriter::complete(
		       size_t accounted_size,
		       const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  int ret;
  NSFSUploadPartInfo info;

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      // test the object is existing
      if (!part_file->exists()) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      Attrs attrs;
      bufferlist bl;
      ret = part_file->read_attrs(rctx.dpp, rctx.y, attrs);
      if (ret < 0) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (!get_attr(attrs, RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  info.num = part_num;
  info.etag = etag;
  info.cksum = cksum;
  info.mtime = set_mtime;

  bufferlist bl;
  encode(info, bl);
  attrs[RGW_NSFS_ATTR_MPUPLOAD] = bl;

  ret = part_file->write_attrs(rctx.dpp, rctx.y, attrs, /*extra_attrs=*/nullptr);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: failed writing attrs for " << part_file->get_name() << dendl;
    return ret;
  }

  ret = part_file->close();
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: failed closing file" << dendl;
    return ret;
  }

  return 0;
}

int NSFSAtomicWriter::prepare(optional_yield y)
{
  int ret;

  ret = obj->make_ent(ObjectType::FILE);
  if (ret < 0) {
    return ret;
  }
  obj->get_obj_attrs(y, dpp);
  obj->close();
  return obj->open(dpp, true, true);
}

int NSFSAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  return obj->write(offset, data, dpp, null_yield);
}

int NSFSAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  int ret;
  uint64_t orig_size = 0;
  auto exists = obj->check_exists(dpp);
  if (exists) {
    orig_size = obj->get_size();
  }

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      if (!exists) {
	return -ENOENT;
      }
    } else {
      if (!exists) {
	return -ENOENT;
      }
      bufferlist bl;
      if (!get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      std::string if_match_str = rgw_string_unquote(if_match);
      if (if_match_str.compare(0, bl.length(), bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (if_nomatch) {
    if (strcmp(if_nomatch, "*") == 0) {
      if (exists) {
	return -ERR_PRECONDITION_FAILED;
      }
    } else {
      bufferlist bl;
      if (get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
        if (if_nomatch_str.compare(0, bl.length(), bl.c_str(), bl.length()) == 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  /* owner is already in attrs[RGW_ATTR_ACL] from the generic layer */

  obj->set_attrs(attrs);
  ret = obj->write_attrs(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: NSFSAtomicWriter failed writing attrs for "
                       << obj->get_name() << dendl;
    return ret;
  }

  NSFSBucket *b = static_cast<NSFSBucket*>(obj->get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << obj->get_name() << dendl;
    return -EINVAL;
  }

  const auto& binfo = b->get_info();
  bool versioned = binfo.versioned();
  bool ver_enabled = binfo.versioning_enabled();
  bool did_demote = false;
  std::string demoted_ver_id;

  /* versioned PUT: demote current version to .versions/ before publishing */
  if (versioned) {
    nsfs::Directory* parent = obj->get_fsent()
      ? obj->get_fsent()->get_parent() : nullptr;
    int parent_fd = -1;
    std::string cur_leaf;

    if (parent) {
      parent_fd = parent->get_fd();
      if (parent_fd < 0) {
        parent->open(dpp);
        parent_fd = parent->get_fd();
      }
      cur_leaf = obj->get_fsent()->get_name();
    }

    if (parent_fd >= 0) {
      int vfd = open_versions_dir(parent_fd);
      if (vfd >= 0) {
        auto vlock = driver->get_fs_strategy()->version_lock(
          dpp, open_versions_lockfile(parent_fd));

        /* in suspended mode, remove any existing _null from .versions/ */
        if (!ver_enabled) {
          std::string null_name = nsfs_ver_entry(cur_leaf, NULL_VERSION_ID);
          ::unlinkat(vfd, null_name.c_str(), 0);
        }

        /* demote current under lock — fresh stat for accurate state */
        struct statx cur_stx;
        if (statx(parent_fd, cur_leaf.c_str(), AT_SYMLINK_NOFOLLOW,
                  STATX_ALL, &cur_stx) == 0) {
          bool cur_is_null = false;
          {
            int chk_fd = ::openat(parent_fd, cur_leaf.c_str(), O_RDONLY);
            if (chk_fd >= 0) {
              cur_is_null = is_null_version_fd(chk_fd);
              ::close(chk_fd);
            }
          }
          if (!(!ver_enabled && cur_is_null)) {
            std::string cur_ver_id = cur_is_null
              ? NULL_VERSION_ID
              : nsfs_version_id_from_statx(cur_stx);
            std::string ver_name = nsfs_ver_entry(cur_leaf, cur_ver_id);

            SafeResult sr = driver->get_fs_strategy()->safe_link(
                                      dpp, parent_fd, cur_leaf,
                                      vfd, ver_name,
                                      statx_mtime_ns(cur_stx),
                                      cur_stx.stx_ino);
            if (sr == SafeResult::OK) {
              int demoted_fd = ::openat(vfd, ver_name.c_str(), O_RDONLY);
              if (demoted_fd >= 0) {
                auto now_ms = std::to_string(
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                std::string ts_x =
                  NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_NON_CURRENT_TS;
                ::fsetxattr(demoted_fd, ts_x.c_str(),
                            now_ms.c_str(), now_ms.size(), 0);
                ::close(demoted_fd);
              }
              demoted_ver_id = cur_ver_id;
              did_demote = true;
            } else if (sr == SafeResult::MISMATCH && cur_is_null) {
              /* null version already demoted by a prior writer */
            } else if (sr != SafeResult::OK) {
              ldpp_dout(dpp, 0) << "ERROR: versioned PUT demote failed for "
                << cur_leaf << " sr=" << (int)sr << dendl;
              ::close(vfd);
              return -ERR_INTERNAL_ERROR;
            }
          }
        }

        ret = obj->link_temp_file(rctx.dpp, rctx.y);
        ::close(vfd);
      } else {
        ret = obj->link_temp_file(rctx.dpp, rctx.y);
      }
    } else {
      ret = obj->link_temp_file(rctx.dpp, rctx.y);
    }
  } else {
    ret = obj->link_temp_file(rctx.dpp, rctx.y);
  }

  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed writing temp file"
                       << dendl;
    return ret;
  }

  /* versioned PUT: link_temp_file already set the version_id xattr,
   * set_instance, and added the new current cache entry; handle the
   * demoted entry here */
  if (versioned && did_demote) {
    auto* bcache = driver->get_bucket_cache();
    std::string obj_name = obj->get_key().get_index_key_name();

    cls_rgw_obj_key old_key;
    old_key.name = obj_name;
    old_key.instance = demoted_ver_id;
    bcache->remove_entry(rctx.dpp, b->get_name(), old_key);

    rgw_bucket_dir_entry dem_bde{};
    dem_bde.key.name = obj_name;
    dem_bde.key.instance = demoted_ver_id;
    dem_bde.ver.pool = 1;
    dem_bde.ver.epoch = 1;
    dem_bde.exists = true;
    dem_bde.meta.category = RGWObjCategory::Main;
    dem_bde.flags = rgw_bucket_dir_entry::FLAG_VER;
    bcache->add_entry(rctx.dpp, b->get_name(), dem_bde);
  }

  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            (exists ? 0 : 1), orig_size, accounted_size);

  return 0;
}

int NSFSLifecycle::get_entry(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const std::string& marker,
                            LCEntry& entry)
{
  return driver->get_user_db()->get_entry(oid, marker, entry);
}

int NSFSLifecycle::get_next_entry(const DoutPrefixProvider* dpp, optional_yield y,
				  const std::string& oid, const std::string& marker,
                                  LCEntry& entry)
{
  return driver->get_user_db()->get_next_entry(oid, marker, entry);
}

int NSFSLifecycle::set_entry(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->set_entry(oid, entry);
}

int NSFSLifecycle::list_entries(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& oid, const std::string& marker,
                                uint32_t max_entries, std::vector<LCEntry>& entries)
{
  return driver->get_user_db()->list_entries(oid, marker, max_entries, entries);
}

int NSFSLifecycle::rm_entry(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->rm_entry(oid, entry);
}

int NSFSLifecycle::get_head(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, LCHead& head)
{
  return driver->get_user_db()->get_head(oid, head);
}

int NSFSLifecycle::put_head(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const LCHead& head)
{
  return driver->get_user_db()->put_head(oid, head);
}

std::unique_ptr<LCSerializer> NSFSLifecycle::get_serializer(const std::string& lock_name,
                                                            const std::string& oid,
                                                            const std::string& cookie)
{
  return std::make_unique<LCNSFSSerializer>(driver, oid, lock_name, cookie);
}

void NSFSDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  mgr->register_resource("user", new RGWRESTMgr_User);
  /* TODO: register "bucket" once rgw_rest_bucket is decoupled from rados */
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newNSFSDriver(CephContext *cct)
{
  rgw::sal::NSFSDriver* driver = new rgw::sal::NSFSDriver(cct);

  int ret = -1;
  const static std::string tenant = "default_ns";
  if ((ret = driver->get_user_db()->Initialize("", -1)) < 0) {
    ldout(cct, 0) << "DB initialization failed for tenant("<<tenant<<")" << dendl;
    return nullptr;
  }

  driver->set_context(cct);

  return driver;
}

}
