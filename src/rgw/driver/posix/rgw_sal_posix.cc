// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "rgw_sal_posix.h"
#include <dirent.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>
#include "rgw_multi.h"
#include "include/scope_guard.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

const int64_t READ_SIZE = 128 * 1024;
const std::string ATTR_PREFIX = "user.X-RGW-";
#define RGW_POSIX_ATTR_BUCKET_INFO "POSIX-Bucket-Info"
#define RGW_POSIX_ATTR_MPUPLOAD "POSIX-Multipart-Upload"
#define RGW_POSIX_ATTR_OWNER "POSIX-Owner"
#define RGW_POSIX_ATTR_OBJECT_TYPE "POSIX-Object-Type"
const std::string mp_ns = "multipart";
const std::string MP_OBJ_PART_PFX = "part-";
const std::string MP_OBJ_HEAD_NAME = MP_OBJ_PART_PFX + "00000";

struct POSIXOwner {
  rgw_user user;
  std::string display_name;

  POSIXOwner() {}

  POSIXOwner(const rgw_user& _u, const std::string& _n) :
    user(_u),
    display_name(_n)
    {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(user, bl);
    encode(display_name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(user, bl);
    decode(display_name, bl);
    DECODE_FINISH(bl);
  }
  friend inline std::ostream &operator<<(std::ostream &out,
                                         const POSIXOwner &o) {
    out << o.user << ":" << o.display_name;
    return out;
  }
};
WRITE_CLASS_ENCODER(POSIXOwner);

std::string get_key_fname(rgw_obj_key& key, bool use_version)
{
  std::string oid;
  if (use_version) {
    oid = key.get_oid();
  } else {
    oid = key.get_index_key_name();
  }
  std::string fname = url_encode(oid, true);

  if (!key.get_ns().empty()) {
    /* Namespaced objects are hidden */
    fname.insert(0, 1, '.');
  }

  return fname;
}

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
  std::string bname;

  if (ns)
    bname = "." + *ns + "_" + url_encode(name, true);
  else
    bname = url_encode(name, true);

  return bname;
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

static inline rgw_obj_key decode_obj_key(const char* fname)
{
  std::string dname, oname, ns;
  dname = url_decode(fname);
  rgw_obj_key key;
  rgw_obj_key::parse_raw_oid(dname, &key);
  return key;
}

static inline rgw_obj_key decode_obj_key(const std::string& fname)
{
  return decode_obj_key(fname.c_str());
}

int decode_owner(Attrs& attrs, POSIXOwner& owner)
{
  bufferlist bl;
  if (!decode_attr(attrs, RGW_POSIX_ATTR_OWNER, owner)) {
    return -EINVAL;
  }

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
    std::string key(keyptr);
    std::string::size_type prefixloc = key.find(ATTR_PREFIX);

    if (prefixloc == std::string::npos) {
      /* Not one of our attributes */
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    /* Make a key that has just the attribute name */
    key.erase(prefixloc, ATTR_PREFIX.length());

    vallen = fgetxattr(fd, keyptr, nullptr, 0);
    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (vallen == 0) {
      /* No attribute value for this name */
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

  attrname = ATTR_PREFIX + key;

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
  std::string attrname{ATTR_PREFIX + key};

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
      return -ENOTEMPTY;
    }

    ret = statx(dir_fd, entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << entry->d_name
                        << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }

    if (S_ISDIR(stx.stx_mode)) {
      /* Recurse */
      ret = delete_directory(dir_fd, entry->d_name, true, dpp);
      if (ret < 0) {
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
      return -ret;
    }
  }

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

  /* Set the type */
  bufferlist type_bl;
  ObjectType type{get_type()};
  type.encode(type_bl);
  attrs[RGW_POSIX_ATTR_OBJECT_TYPE] = type_bl;

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

int FSEnt::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  rgw_bucket_dir_entry bde{};

  rgw_obj_key key = decode_obj_key(get_name());
  if (parent->get_type() == ObjectType::MULTIPART) {
    key.ns = mp_ns;
  }
  key.get_index_key(&bde.key);
  bde.ver.pool = 1;
  bde.ver.epoch = 1;

  switch (parent->get_type().type) {
    case ObjectType::VERSIONED:
      bde.flags = rgw_bucket_dir_entry::FLAG_VER;
      bde.exists = true;
      if (!key.have_instance()) {
	  bde.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
      }
      break;
    case ObjectType::MULTIPART:
    case ObjectType::DIRECTORY:
      bde.exists = true;
      break;
    case ObjectType::UNKNOWN:
    case ObjectType::FILE:
    case ObjectType::SYMLINK:
      return -EINVAL;
  }

  Attrs attrs;
  int ret = open(dpp);
  if (ret < 0)
    return ret;

  ret = get_x_attrs(y, dpp, get_fd(), attrs, get_name());
  if (ret < 0)
    return ret;

  POSIXOwner o;
  ret = decode_owner(attrs, o);
  if (ret < 0) {
    bde.meta.owner = "unknown";
    bde.meta.owner_display_name = "unknown";
  } else {
    bde.meta.owner = o.user.to_str();
    bde.meta.owner_display_name = o.display_name;
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

  int ret = ::fsync(fd);
  if(ret < 0) {
    return ret;
  }

  ret = ::close(fd);
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
  off64_t scount = 0, dcount = 0;

  int ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not stat source file " << get_name()
                      << dendl;
    return ret;
  }

  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not open source file " << get_name()
                      << dendl;
    return ret;
  }

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

  std::unique_ptr<File> dest = clone();
  dest->parent = dst_dir;
  dest->fname = dst_name;

  ret = dest->create(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not create dest file "
                      << dest->get_name() << dendl;
    return ret;
  }
  ret = dest->open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not open dest file "
                      << dest->get_name() << dendl;
    return ret;
  }

  ret = copy_file_range(fd, &scount, dest->get_fd(), &dcount, get_size(), 0);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not copy object " << dest->get_name()
                      << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int File::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  if (!exists()) {
    return 0;
  }

  int ret = unlinkat(parent->get_fd(), fname.c_str(), 0);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name()
                        << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  return 0;
}

int File::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y, std::string temp_fname)
{
  if (fd < 0) {
    return 0;
  }

  char temp_file_path[PATH_MAX];
  // Only works on Linux - Non-portable
  snprintf(temp_file_path, PATH_MAX,  "/proc/self/fd/%d", fd);

  int ret = linkat(AT_FDCWD, temp_file_path, parent->get_fd(), temp_fname.c_str(), AT_SYMLINK_FOLLOW);
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: linkat for temp file could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }

  ret = renameat(parent->get_fd(), temp_fname.c_str(), parent->get_fd(), get_name().c_str());
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: renameat for object could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
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
    ObjectType type{ObjectType::MULTIPART};
    int tmpfd;
    Attrs attrs;

    tmpfd = openat(get_fd(), name.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (tmpfd > 0) {
      ret = get_x_attrs(y, dpp, tmpfd, attrs, name);
      if (ret >= 0) {
        decode_attr(attrs, RGW_POSIX_ATTR_OBJECT_TYPE, type);
      }
    }
    switch (type.type) {
    case ObjectType::VERSIONED:
      nent = std::make_unique<VersionedDirectory>(name, this, instance, nstx, ctx);
      break;
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
  } else if (S_ISLNK(nstx.stx_mode)) {
    nent = std::make_unique<Symlink>(name, this, nstx, ctx);
  } else {
    return -EINVAL;
  }

  ent.swap(nent);
  return 0;
}

int Directory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                          fill_cache_cb_t &cb)
{
  int ret = for_each(dpp, [this, &cb, &dpp, &y](const char *name) {
    std::unique_ptr<FSEnt> ent;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    int ret = get_ent(dpp, y, name, std::string(), ent);
    if (ret < 0)
      return ret;

    ent->stat(dpp); // Stat the object to get the type

    ret = ent->fill_cache(dpp, y, cb);
    if (ret < 0)
      return ret;
    return 0;
  });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list directory " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return ret;
  }

  return 0;
}

int Symlink::create(const DoutPrefixProvider* dpp, bool* existed, bool temp_file)
{
  if (temp_file) {
    ldpp_dout(dpp, 0) << "ERROR: cannot create symlink with temp_file " << get_name() << dendl;
    return -EINVAL;
  }

  int ret = symlinkat(target->get_name().c_str(), parent->get_fd(), fname.c_str());
  if (ret < 0) {
    ret = errno;
    if (ret == EEXIST && existed != nullptr) {
      *existed = true;
    }
    ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int Symlink::fill_target(const DoutPrefixProvider *dpp, Directory* parent, std::string sname, std::string tname, std::unique_ptr<FSEnt>& ent, CephContext* _ctx)
{
  int ret;

  if (!tname.empty()) {
      ret = parent->get_ent(dpp, null_yield, tname, std::string(), ent);
      if (ret < 0) {
	ent = std::make_unique<File>(tname, parent, _ctx);
      }
      return 0;
  }

  char link[PATH_MAX];
  memset(link, 0, sizeof(link));
  ret = readlinkat(parent->get_fd(), sname.c_str(), link, sizeof(link));
  if (ret < 0) {
    ret = errno;
    return -ret;
  }
  ret = parent->get_ent(dpp, null_yield, link, std::string(), ent);
  if (ret < 0) {
    ent = std::make_unique<File>(link, parent, _ctx);
  }
  return 0;
}

int Symlink::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = FSEnt::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  if (!S_ISLNK(stx.stx_mode)) {
    /* Not a symlink */
    ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " is not a symlink" << dendl;
    return -EINVAL;
  }

  struct statx sstx;
  ret = statx(parent->get_fd(), fname.c_str(), 0, STATX_BASIC_STATS, &sstx);
  if (ret >= 0) {
    stx.stx_size = sstx.stx_size;
  }

  exist = true;
  return fill_target(dpp, parent, get_name(), std::string(), target, ctx);
}

int Symlink::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  rgw_bucket_dir_entry bde{};
  int ret;

  rgw_obj_key key = decode_obj_key(get_name());
  key.get_index_key(&bde.key);
  bde.ver.pool = 1;
  bde.ver.epoch = 1;

  bde.flags = rgw_bucket_dir_entry::FLAG_VER;
  bde.exists = true;
  bde.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;

  if (!target) {
    ret = stat(dpp, /*force=*/false);
    if (ret < 0)
      return ret;
  }

  Attrs attrs;
  ret = target->read_attrs(dpp, y, attrs);
  if (ret < 0)
    return ret;

  POSIXOwner o;
  ret = decode_owner(attrs, o);
  if (ret < 0) {
    bde.meta.owner = "unknown";
    bde.meta.owner_display_name = "unknown";
  } else {
    bde.meta.owner = o.user.to_str();
    bde.meta.owner_display_name = o.display_name;
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
  }

  return cb(dpp, bde);
}

int Symlink::read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs)
{
  if (target)
    return target->read_attrs(dpp, y, attrs);

  return FSEnt::read_attrs(dpp, y, attrs);
}

int Symlink::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name)
{
  int ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not stat source file " << get_name()
                      << dendl;
    return ret;
  }
  rgw_obj_key skey = decode_obj_key(target->get_name());
  rgw_obj_key dkey = decode_obj_key(dst_name);
  dkey.instance = skey.instance;
  std::string tgtname = get_key_fname(dkey, /*use_version=*/true);

  ret = symlinkat(tgtname.c_str(), dst_dir->get_fd(), dst_name.c_str());

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
                          fill_cache_cb_t &cb)
{
  int ret = FSEnt::fill_cache(dpp, y, cb);
  if (ret < 0)
    return ret;

  return Directory::fill_cache(dpp, y, cb);
}

int VersionedDirectory::open(const DoutPrefixProvider* dpp)
{
  if (fd > 0) {
    return 0;
  }
  int ret = Directory::open(dpp);
  if (ret < 0) {
    return 0;
  }

  if (!instance_id.empty()) {
    rgw_obj_key key = decode_obj_key(get_name());
    key.instance = instance_id;
    get_ent(dpp, null_yield, get_key_fname(key, /*use_version=*/true), std::string(), cur_version);
  }

  if (!cur_version) {
    /* Can't open File, probably doesn't exist yet */
    return 0;
  }

  return cur_version->open(dpp);
}

int VersionedDirectory::create(const DoutPrefixProvider* dpp, bool* existed, bool temp_file)
{
  int ret = mkdirat(parent->get_fd(), fname.c_str(), S_IRWXU);
  if (ret < 0) {
    ret = errno;
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create versioned directory " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not open versioned directory " << get_name()
                      << dendl;
    return ret;
  }

  /* Need type attribute written */
  Attrs attrs;
  ret = write_attrs(dpp, null_yield, attrs, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not write attrs for versioned directory " << get_name()
                      << dendl;
    return ret;
  }

  if (temp_file) {
    /* Want to create an actual versioned object */
    rgw_obj_key key = decode_obj_key(get_name());
    key.instance = instance_id;
    std::unique_ptr<FSEnt> file = 
        std::make_unique<File>(get_key_fname(key, /*use_version=*/true), this, ctx);
    ret = add_file(dpp, std::move(file), existed, temp_file);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

std::string VersionedDirectory::get_new_instance()
{
  return gen_rand_instance_name();
}

int VersionedDirectory::add_file(const DoutPrefixProvider* dpp, std::unique_ptr<FSEnt>&& file, bool* existed, bool temp_file)
{
  int ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not open versioned directory " << get_name()
                      << dendl;
    return ret;
  }

  ret = file->create(dpp, existed, temp_file);
  if (ret < 0) {
    return ret;
  }

  if (!temp_file) {
    return set_cur_version_ent(dpp, file.get());
  }

  cur_version = std::move(file);
  return 0;
}

int VersionedDirectory::set_cur_version_ent(const DoutPrefixProvider* dpp, FSEnt* file)
{
  /* Delete current version symlink */
  std::unique_ptr<FSEnt> del;
  int ret = get_ent(dpp, null_yield, get_name(), std::string(), del);
  if (ret >= 0) {
    ret = del->remove(dpp, null_yield, /*delete_children=*/true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove cur_version " << get_name()
                        << dendl;
      return ret;
    }
  }

  /* Create new current version symlink */
  std::unique_ptr<Symlink> sl =
      std::make_unique<Symlink>(get_name(), this, file->get_name(), ctx);
  ret = sl->create(dpp, /*existed=*/nullptr, /*temp_file=*/false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not create cur_version symlink "
                      << get_name() << dendl;
    return ret;
  }

  return 0;
}

int VersionedDirectory::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = Directory::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  ret = open(dpp);
  if (ret < 0)
    return ret;

  if (cur_version) {
    /* Already have a File for the current version, use it */
    ret = cur_version->stat(dpp);
    if (ret < 0)
      return ret;
    stx.stx_size = cur_version->get_stx().stx_size;

    return 0;
  }

  /* Try to read the symlink */
  std::unique_ptr<Symlink> sl = std::make_unique<Symlink>(get_name(), this, ctx);
  ret = sl->stat(dpp);
  if (ret < 0) {
    if (ret == -ENOENT)
      return 0;
    return ret;
  }

  if (!sl->exists()) {
    stx.stx_size = 0;
    return 0;
  }

  cur_version = sl->get_target()->clone_base();
  ret = cur_version->open(dpp);
  if (ret < 0) {
    /* If target doesn't exist, it's a delete marker */
    cur_version.reset();
    stx.stx_size = 0;
    return 0;
  }
  ret = cur_version->stat(dpp);
  if (ret < 0)
    return ret;
  stx.stx_size = cur_version->get_stx().stx_size;

  return 0;
}

int VersionedDirectory::read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs)
{
  if (!cur_version)
    return FSEnt::read_attrs(dpp, y, attrs);

  int ret = cur_version->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  /* Override type, it should be VERSIONED */
  bufferlist type_bl;
  ObjectType type{get_type()};
  type.encode(type_bl);
  attrs[RGW_POSIX_ATTR_OBJECT_TYPE] = type_bl;

  return 0;
}

int VersionedDirectory::write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs)
{
  if (cur_version) {
    int ret = cur_version->write_attrs(dpp, y, attrs, extra_attrs);
    if (ret < 0)
      return ret;
  }

  return FSEnt::write_attrs(dpp, y, attrs, extra_attrs);
}

int VersionedDirectory::write(int64_t ofs, bufferlist &bl,
                              const DoutPrefixProvider *dpp, optional_yield y)
{
  if (!cur_version)
    return 0;
  return cur_version->write(ofs, bl, dpp, y);
}

int VersionedDirectory::read(int64_t ofs, int64_t left, bufferlist &bl,
                    const DoutPrefixProvider *dpp, optional_yield y)
{
  if (!cur_version)
    return 0;
  return cur_version->read(ofs, left, bl, dpp, y);
}

int VersionedDirectory::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
                              std::string temp_fname)
{
  if (!cur_version)
    return -EINVAL;
  int ret = cur_version->link_temp_file(dpp, y, temp_fname);
  if (ret < 0)
    return ret;

  return set_cur_version_ent(dpp, cur_version.get());
}

int VersionedDirectory::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name)
{
  int ret;
  rgw_obj_key dest_key = decode_obj_key(dst_name);
  std::string basename = get_key_fname(dest_key, /*use_version=*/false);

  // Delete the target
  {
    std::unique_ptr<FSEnt> del;
    ret = dst_dir->get_ent(dpp, y, basename, std::string(), del);
    if (ret >= 0) {
      ret = del->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << basename
                          << dendl;
        return ret;
      }
    }
  }

  ret = dst_dir->open(dpp);
  std::unique_ptr<VersionedDirectory> dest = clone();
  dest->parent = dst_dir;
  dest->fname = basename;

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

  std::string tgtname;
  ret = for_each(dpp, [this, &dest, &dest_key, &tgtname, &dpp, &y](const char* name) {
    std::unique_ptr<FSEnt> sobj;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }
    rgw_obj_key key = decode_obj_key(name);
    if (!dest_key.instance.empty() && dest_key.instance != key.instance) {
      /* Were asked to copy a single version, and this is not it */
      return 0;
    }

    int r = this->get_ent(dpp, y, name, std::string(), sobj);
    if (r < 0)
      return r;
    key.name = dest_key.name;
    tgtname = get_key_fname(key, /*use_version=*/true);
    return sobj->copy(dpp, y, dest.get(), tgtname);
  });

  if (!dest_key.instance.empty()) {
    /* We didn't copy the symlink, make a new one */
    std::unique_ptr<Symlink> sl = std::make_unique<Symlink>(basename, dest.get(), tgtname, ctx);
    ret = sl->create(dpp, /*existed=*/nullptr, /*temp_file=*/false);
  }

  return ret;
}

int VersionedDirectory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  std::string tgtname;
  bool newlink = false;

  int ret = open(dpp);
  if (ret < 0)
    return ret;

  if (instance_id.empty()) {
    /* Check if directory is empty */
    ret = for_each(dpp, [](const char *n) {
      return -ENOENT;
    });

    if (ret == 0) {
      /* We're empty, nuke us */
      return Directory::remove(dpp, y, /*delete_children=*/true);
    }

    /* Add a delete marker */
    rgw_obj_key key = decode_obj_key(get_name());
    key.instance = gen_rand_instance_name();
    tgtname = get_key_fname(key, /*use_version=*/true);
    newlink = true;
    ret = remove_symlink(dpp, y);
    if (ret < 0) {
      return ret;
    }
  } else {
    /* Delete specific version */
    rgw_obj_key key = decode_obj_key(get_name());
    key.instance = instance_id;
    std::string name = get_key_fname(key, /*use_version=*/true);

    std::unique_ptr<FSEnt> f;
    ret = get_ent(dpp, y, name, std::string(), f);
    if (ret == 0) {
      ret = f->stat(dpp);
      if (ret < 0)
        return ret;
      ret = f->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0)
        return ret;
    } else if (ret == -ENOENT) {
      /* See if we're removing a delete marker */
      std::unique_ptr<Symlink> sl =
          std::make_unique<Symlink>(get_name(), this, ctx);
      ret = sl->stat(dpp);
      if (ret == 0) {
        if (name != sl->get_target()->get_name()) {
	  /* Symlink didn't match, don't change anything */
	  return 0;
	}
      }
      /* FALLTHROUGH */
    } else {
      return ret;
    }

    /* Possibly move symlink */
    ret = remove_symlink(dpp, y, name);
    if (ret < 0) {
      if (ret == -ENOKEY) {
        return 0;
      }
      return ret;
    }
    newlink = true;
    /* Create new current version symlink */
    ret = for_each(dpp, [&tgtname](const char *n) {
      if (n[0] == '.') {
        /* Skip dotfiles */
        return 0;
      }

      tgtname = n;
      return 0;
    });

    if (tgtname.empty()) {
      /* We're empty, nuke us */
      exist = false;
      return Directory::remove(dpp, y, /*delete_children=*/true);
    }
  }

  if (newlink) {
    exist = true;
    std::unique_ptr<Symlink> sl =
        std::make_unique<Symlink>(get_name(), this, tgtname, ctx);
    return sl->create(dpp, /*existed=*/nullptr, /*temp_file=*/false);
  }
  return 0;
}

int VersionedDirectory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                          fill_cache_cb_t &cb)
{
  int ret = for_each(dpp, [this, &cb, &dpp, &y](const char *name) {
    std::unique_ptr<FSEnt> ent;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    int ret = get_ent(dpp, y, name, std::string(), ent);
    if (ret < 0)
      return ret;

    ent->stat(dpp); // Stat the object to get the type

    ret = ent->fill_cache(dpp, y, cb);
    if (ret < 0)
      return ret;
    return 0;
  });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list directory " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return ret;
  }

  return 0;
}

std::string VersionedDirectory::get_cur_version()
{
  if (!cur_version)
    return "";

  rgw_obj_key key = decode_obj_key(cur_version->get_name());

  return key.instance;
}

int VersionedDirectory::remove_symlink(const DoutPrefixProvider *dpp, optional_yield y, std::string match)
{
  int ret;

  std::unique_ptr<Symlink> sl =
      std::make_unique<Symlink>(get_name(), this, ctx);
  ret = sl->stat(dpp);
  if (ret < 0) {
    /* Doesn't exist, nothing to do */
    if (ret == -ENOENT)
      return 0;
    return ret;
  }

  if (!match.empty()) {
    if (match != sl->get_target()->get_name())
      return -ENOKEY;
  }

  ret = sl->remove(dpp, y, /*delete_children=*/false);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int POSIXDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);

  base_path = g_conf().get_val<std::string>("rgw_posix_base_path");

  ldpp_dout(dpp, 20) << "Initializing POSIX driver: " << base_path << dendl;

  /* ordered listing cache */
  bucket_cache.reset(
    new BucketCache(
      this, base_path,
      g_conf().get_val<std::string>("rgw_posix_database_root"),
      g_conf().get_val<int64_t>("rgw_posix_cache_max_buckets"),
      g_conf().get_val<int64_t>("rgw_posix_cache_lanes"),
      g_conf().get_val<int64_t>("rgw_posix_cache_partitions"),
      g_conf().get_val<int64_t>("rgw_posix_cache_lmdb_count")));

  root_dir = std::make_unique<Directory>(base_path, nullptr, ctx());
  int ret = root_dir->open(dpp);
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
  ldpp_dout(dpp, 20) << "root_fd: " << root_dir->get_fd() << dendl;

  ldpp_dout(dpp, 20) << "SUCCESS" << dendl;
  return 0;
}

std::unique_ptr<User> POSIXDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);

  return std::make_unique<POSIXUser>(std::move(user), this);
}

int POSIXDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

int POSIXDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new POSIXUser(std::move(nu), this);
  user->reset(u);
  return 0;
}

std::unique_ptr<Object> POSIXDriver::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(this, k);
}

int POSIXDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  *bucket = std::make_unique<POSIXBucket>(this, root_dir.get(), b);
  return (*bucket)->load_bucket(dpp, y);
}

std::unique_ptr<Bucket> POSIXDriver::get_bucket(const RGWBucketInfo& i)
{
  /* Don't need to fetch the bucket info, use the provided one */
  return std::make_unique<POSIXBucket>(this, root_dir.get(), i);
}

std::string POSIXDriver::zone_unique_trans_id(const uint64_t unique_num)
{
  char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
  time_t timestamp = time(NULL);

  snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
           (unsigned long long)unique_num,
           (unsigned long long)timestamp);

  return std::string(buf);
}
std::unique_ptr<Writer> POSIXDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, _head_obj,
							   owner, ptail_placement_rule,
							   unique_tag, position,
							   cur_accounted_size);

  return std::make_unique<FilterWriter>(std::move(writer), std::move(_head_obj));
}

std::unique_ptr<Writer> POSIXDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{

  return std::make_unique<POSIXAtomicWriter>(dpp, y, _head_obj, this, owner, ptail_placement_rule, olh_epoch, unique_tag);
}

void POSIXDriver::finalize(void)
{
  next->finalize();
}

void POSIXDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  return next->register_admin_apis(mgr);
}

bool POSIXDriver::process_expired_objects(const DoutPrefixProvider *dpp,
	       				                          optional_yield y) {
  return next->process_expired_objects(dpp, y);
}

std::unique_ptr<Notification> POSIXDriver::get_notification(rgw::sal::Object* obj,
			      rgw::sal::Object* src_obj, struct req_state* s,
			      rgw::notify::EventType event_type, optional_yield y,
			      const std::string* object_name)
{
  return next->get_notification(obj, src_obj, s, event_type, y, object_name);
}

std::unique_ptr<Notification> POSIXDriver::get_notification(
    const DoutPrefixProvider* dpp,
    rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    const rgw::notify::EventTypeList& event_types,
    rgw::sal::Bucket* _bucket,
    std::string& _user_id,
    std::string& _user_tenant,
    std::string& _req_id,
    optional_yield y) {
  return next->get_notification(dpp, obj, src_obj, event_types, _bucket,
                                _user_id, _user_tenant, _req_id, y);
}

// TODO: marker and other params
int POSIXDriver::list_buckets(const DoutPrefixProvider* dpp, const rgw_owner& owner,
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

    RGWBucketEnt ent;
    ent.bucket.name = url_decode(entry->d_name);
    ent.creation_time = ceph::real_clock::from_time_t(stx.stx_btime.tv_sec);
    // TODO: ent.size and ent.count

    result.buckets.push_back(std::move(ent));

    errno = 0;
  }
  ret = errno;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list buckets for " << owner << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int POSIXBucket::create(const DoutPrefixProvider* dpp,
			const CreateParams& params,
			optional_yield y)
{
  info.owner = params.owner;

  info.bucket.marker = params.marker;
  info.bucket.bucket_id = params.bucket_id;

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

int POSIXUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->read_attrs(dpp, y);
}

int POSIXUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int POSIXUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_user(dpp, y);
}

int POSIXUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  return next->store_user(dpp, y, exclusive, old_info);
}

int POSIXUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->remove_user(dpp, y);
}

std::unique_ptr<Object> POSIXBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<POSIXObject>(driver, k, this);
}

int POSIXObject::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  return ent->fill_cache(dpp, y, cb);
}

int POSIXDriver::mint_listing_entry(const std::string &bname,
                                    rgw_bucket_dir_entry &bde) {
    std::unique_ptr<Bucket> b;
    std::unique_ptr<Object> obj;
    POSIXObject *pobj;
    int ret;

    ret = load_bucket(nullptr, rgw_bucket(std::string(), bname),
                      &b, null_yield);
    if (ret < 0)
      return ret;

    obj = b->get_object(decode_obj_key(bde.key.name));
    pobj = static_cast<POSIXObject *>(obj.get());

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
int POSIXBucket::fill_cache(const DoutPrefixProvider* dpp, optional_yield y,
			    fill_cache_cb_t& cb)
{
  return dir->fill_cache(dpp, y, cb);
}

int POSIXBucket::list(const DoutPrefixProvider* dpp, ListParams& params,
		      int max, ListResults& results, optional_yield y)
{
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
	if (params.list_versions && versioned() && bde_key.instance.empty()) {
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

int POSIXBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  for (auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }

  return write_attrs(dpp, y);
}

int POSIXBucket::remove(const DoutPrefixProvider* dpp,
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

int POSIXBucket::remove_bypass_gc(int concurrent_max,
				  bool keep_index_consistent,
				  optional_yield y,
				  const DoutPrefixProvider *dpp)
{
  return remove(dpp, true, y);
}

int POSIXBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
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
  ret = decode_attr(attrs, RGW_POSIX_ATTR_BUCKET_INFO, info);
  if (ret < 0) {
    // TODO dang: fake info up (UID to owner conversion?)
    info = bak_info;
  } else {
    // Don't leave info visible in attributes
    attrs.erase(RGW_POSIX_ATTR_BUCKET_INFO);
  }

  return 0;
}

int POSIXBucket::set_acl(const DoutPrefixProvider* dpp,
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

int POSIXBucket::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
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

int POSIXBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return 0;
}

int POSIXBucket::sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                  RGWBucketEnt* ent)
{
  return 0;
}

int POSIXBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                     uint64_t num_objs, optional_yield y)
{
  return 0;
}

int POSIXBucket::chown(const DoutPrefixProvider* dpp, const rgw_owner& new_owner, optional_yield y)
{
  /* TODO map user to UID/GID, and change it */
  return 0;
}

int POSIXBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
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

int POSIXBucket::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  // Bucket info is stored as an attribute, but not in attrs[]
  bufferlist bl;
  encode(info, bl);
  Attrs extra_attrs;
  extra_attrs[RGW_POSIX_ATTR_BUCKET_INFO] = bl;

  return dir->write_attrs(dpp, y, attrs, &extra_attrs);
}

int POSIXBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return dir->for_each(dpp, [](const char* name) {
    /* for_each filters out "." and "..", so reaching here is not empty */
    return -ENOTEMPTY;
  });
}

int POSIXBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return 0;
}

int POSIXBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y)
{
  *pmtime = mtime;

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return dir->read_attrs(dpp, y, attrs);
}

int POSIXBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			    uint64_t end_epoch, uint32_t max_entries,
			    bool* is_truncated, RGWUsageIter& usage_iter,
			    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int POSIXBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  return 0;
}

int POSIXBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return 0;
}

int POSIXBucket::check_index(const DoutPrefixProvider *dpp, optional_yield y,
                             std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                             std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return 0;
}

int POSIXBucket::rebuild_index(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int POSIXBucket::set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout)
{
  return 0;
}

int POSIXBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

std::unique_ptr<MultipartUpload> POSIXBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<POSIXMultipartUpload>(driver, this, oid, upload_id, owner, mtime);
}

int POSIXBucket::list_multiparts(const DoutPrefixProvider *dpp,
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

    ACLOwner owner;
    std::unique_ptr<MultipartUpload> upload =
        std::make_unique<POSIXMultipartUpload>(
            driver, this, std::string(d_name), std::nullopt, owner,
            real_clock::now());
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

int POSIXBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y)
{
  return 0;
}

int POSIXBucket::create(const DoutPrefixProvider* dpp, optional_yield y, bool* existed)
{
  int ret = dir->create(dpp, existed);
  if (ret < 0) {
    return ret;
  }

  return write_attrs(dpp, y);
}

std::string POSIXBucket::get_fname()
{
  return bucket_fname(get_name(), ns);
}

int POSIXBucket::rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj)
{
  int ret;
  Directory* dst_dir = dir->get_parent();

  info.bucket.name = target_obj->get_key().get_oid();
  ns.reset();

  if (!target_obj->get_instance().empty()) {
    /* This is a versioned object.  Need to handle versioneddirectory */
    POSIXObject *to = static_cast<POSIXObject *>(target_obj);
    ret = to->open(dpp, true, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open target obj " << to->get_name() << dendl;
      return ret;
    }
    dst_dir = static_cast<Directory *>(to->get_fsent());
  }

  return dir->rename(dpp, y, dst_dir, get_fname());
}

int POSIXObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				uint32_t flags,
                                std::list<rgw_obj_index_key>* remove_objs,
				RGWObjVersionTracker* objv)
{
  POSIXBucket *b = static_cast<POSIXBucket*>(get_bucket());
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

  cls_rgw_obj_key key;
  get_key().get_index_key(&key);
  driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);

  if (!key.instance.empty() && !ent->exists()) {
    /* Remove the non-versiond key as well */
    key.instance.clear();
    driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);
  }
  return 0;
}

int POSIXObject::copy_object(const ACLOwner& owner,
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
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  int ret;
  POSIXBucket *db = static_cast<POSIXBucket*>(dest_bucket);
  POSIXBucket *sb = static_cast<POSIXBucket*>(src_bucket);
  POSIXObject *dobj = static_cast<POSIXObject*>(dest_object);

  if (!db || !sb) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket to copy " << get_name()
                      << dendl;
    return -EINVAL;
  }
  bool has_instance = !get_key().instance.empty();

  // Source must exist, and we need to know if it's a shadow obj
  if (!check_exists(dpp)) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  if (!get_key().instance.empty() && !has_instance) {
    /* For copy, no instance meance copy all instances.  Clear intance id if it
     * was passed in clear. */
    get_key().instance.clear();
  }

  if (state.obj != dobj->state.obj) {
    /* An actual copy, copy the data */
    ret = copy(dpp, y, sb, db, dobj);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to copy object " << get_key()
                          << dendl;
        return ret;
    }
  }
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
  if (rgw::sal::get_attr(src_attrs, RGW_POSIX_ATTR_MPUPLOAD, mpu)) {
    attrs[RGW_POSIX_ATTR_MPUPLOAD] = mpu;
  }
  bufferlist ownerbl;
  if (rgw::sal::get_attr(src_attrs, RGW_POSIX_ATTR_OWNER, ownerbl)) {
    attrs[RGW_POSIX_ATTR_OWNER] = ownerbl;
  }
  bufferlist pot;
  if (rgw::sal::get_attr(src_attrs, RGW_POSIX_ATTR_OBJECT_TYPE, pot)) {
    attrs[RGW_POSIX_ATTR_OBJECT_TYPE] = pot;
  }
  return dobj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
}

int POSIXObject::list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			    int max_parts, int marker, int* next_marker,
			    bool* truncated, list_parts_each_t each_func,
			    optional_yield y)
{
  return -EOPNOTSUPP;
}

bool POSIXObject::is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                                    const ceph::real_time& obj_mtime)
{
  return false;
}

int POSIXObject::load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh)
{
  int ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int POSIXObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags)
{
  if (delattrs) {
    for (auto& it : *delattrs) {
      if (it.first == RGW_POSIX_ATTR_OBJECT_TYPE) {
	// Don't delete type
	continue;
      }
      state.attrset.erase(it.first);
    }
  }
  if (setattrs) {
    for (auto& it : *setattrs) {
      if (it.first == RGW_POSIX_ATTR_OBJECT_TYPE) {
	// Don't overwrite type
	continue;
      }
      state.attrset[it.first] = it.second;
    }
  }

  write_attrs(dpp, y);
  return 0;
}

int POSIXObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                                rgw_obj* target_obj)
{
  //int fd;

  int ret = open(dpp, false);
  if (ret < 0) {
    return ret;
  }

  ret = ent->read_attrs(dpp, y, state.attrset);
  if (ret == 0)
    state.has_attrs = true;
  else
    state.has_attrs = false;

  return ret;
}

int POSIXObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp, uint32_t flags)
{
  state.attrset[attr_name] = attr_val;
  return write_attrs(dpp, y);
}

int POSIXObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
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

bool POSIXObject::is_expired()
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

void POSIXObject::gen_rand_obj_instance_name()
{
  state.obj.key.set_instance(gen_rand_instance_name());
}

std::unique_ptr<MPSerializer> POSIXObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return std::make_unique<MPPOSIXSerializer>(dpp, driver, this, lock_name);
}

int MPPOSIXSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y)
{
  if (!obj->check_exists(dpp)) {
    return -ENOENT;
  }

  POSIXBucket* b = static_cast<POSIXBucket*>(obj->get_bucket());
  if (b->get_dir()->get_type() == ObjectType::MULTIPART && b->get_dir_fd(dpp) > 0) {
    return 0;
  }

  return -ENOENT;
}

int POSIXObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y,
                            uint32_t flags)
{
  return -ERR_NOT_IMPLEMENTED;
}

int POSIXObject::transition_to_cloud(Bucket* bucket,
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

int POSIXObject::restore_obj_from_cloud(Bucket* bucket,
          rgw::sal::PlacementTier* tier,
	  CephContext* cct,
          std::optional<uint64_t> days,
          bool& in_progress,
          const DoutPrefixProvider* dpp, 
          optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

bool POSIXObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return (r1 == r2);
}

int POSIXObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
    return 0;
}

int POSIXObject::swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

int POSIXObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  POSIXBucket *b = static_cast<POSIXBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }
  /* TODO Get UID from user */
  int uid = 0;
  int gid = 0;

  int ret = fchownat(b->get_dir_fd(dpp), get_fname(/*use_version=*/true).c_str(), uid, gid, AT_SYMLINK_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  return 0;
}

int POSIXObject::get_cur_version(const DoutPrefixProvider* dpp, rgw_obj_key& key)
{
  return 0;
}

int POSIXObject::set_cur_version(const DoutPrefixProvider *dpp)
{
  VersionedDirectory* vdir = static_cast<VersionedDirectory*>(ent.get());
  std::unique_ptr<FSEnt> child;
  int ret = vdir->get_ent(dpp, null_yield, get_fname(true), std::string(), child);
  if (ret < 0)
    return ret;

  ret = vdir->set_cur_version_ent(dpp, child.get());
  return ret;
}

int POSIXObject::stat(const DoutPrefixProvider* dpp)
{
  int ret;

  if (!ent) {
    ret = static_cast<POSIXBucket *>(bucket)->get_dir()->get_ent(
        dpp, null_yield, get_fname(/*use_version=*/false), state.obj.key.instance, ent);
    if (ret < 0) {
      state.exists = false;
      return ret;
    }
  }

  ret = ent->stat(dpp);
  if (ret < 0) {
    state.exists = false;
    return ret;
  }

  if (state.obj.key.instance.empty()) {
    state.obj.key.instance = ent->get_cur_version();
  }

  state.exists = ent->exists();
  if (!state.exists) {
    return 0;
  }

  state.accounted_size = state.size = ent->get_stx().stx_size;
  state.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);

  return 0;
}

int POSIXObject::make_ent(ObjectType type)
{
  if (ent)
    return 0;

  switch (type.type) {
    case ObjectType::UNKNOWN:
      return -EINVAL;
    case ObjectType::FILE:
      ent = std::make_unique<File>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case ObjectType::DIRECTORY:
      ent = std::make_unique<Directory>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case ObjectType::SYMLINK:
      ent = std::make_unique<Symlink>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case ObjectType::MULTIPART:
      ent = std::make_unique<MPDirectory>(
          get_fname(/*use_version=*/true), static_cast<POSIXBucket *>(bucket)->get_dir(), driver->ctx());
      break;
    case ObjectType::VERSIONED:
      ent = std::make_unique<VersionedDirectory>(
          get_fname(/*use_version=*/false), static_cast<POSIXBucket *>(bucket)->get_dir(), get_instance(), driver->ctx());
      break;
  }

  return 0;
}

int POSIXObject::get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner)
{
  POSIXOwner o;
  int ret = decode_owner(get_attrs(), o);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__
        << ": No " RGW_POSIX_ATTR_OWNER " attr" << dendl;
    return ret;
  }

  *owner = driver->get_user(o.user);
  (*owner)->load_user(dpp, y);
  return 0;
}

std::unique_ptr<Object::ReadOp> POSIXObject::get_read_op()
{
  return std::make_unique<POSIXReadOp>(this);
}

std::unique_ptr<Object::DeleteOp> POSIXObject::get_delete_op()
{
  return std::make_unique<POSIXDeleteOp>(this);
}

int POSIXObject::open(const DoutPrefixProvider* dpp, bool create, bool temp_file)
{
  int ret{0};

  if (!ent) {
    ret = stat(dpp);
    if (ret < 0) {
      if (!create) {
	return ret;
      }
      if (versioned()) {
        ret = make_ent(ObjectType::VERSIONED);
      } else {
        ret = make_ent(ObjectType::FILE);
      }
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

int POSIXObject::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string temp_fname = gen_temp_fname();
  int ret = ent->link_temp_file(dpp, y, temp_fname);
  if (ret < 0)
    return ret;
 
  POSIXBucket *b = static_cast<POSIXBucket *>(get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name()
		      << dendl;
    return -EINVAL;
  }

  fill_cache( nullptr, null_yield,
      [&](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &bde) -> int {
	driver->get_bucket_cache()->add_entry(dpp, b->get_name(), bde);
	return 0;
      });
  return 0;
}


int POSIXObject::close()
{
  if (ent)
    return ent->close();

  return 0;
}

int POSIXObject::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!ent)
    return -ENOENT;
  return ent->read(ofs, left, bl, dpp, y);
}

int POSIXObject::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  return ent->write(ofs, bl, dpp, y);
}

int POSIXObject::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return ent->write_attrs(dpp, y, state.attrset, nullptr);
}

int POSIXObject::POSIXReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = source->stat(dpp);
  if (ret < 0)
    return ret;

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

int POSIXObject::POSIXReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  return source->read(ofs, end + 1, bl, dpp, y);
}

int POSIXObject::generate_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  ret = generate_etag(dpp, y);
  return ret;
}

int POSIXObject::generate_mp_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int POSIXObject::generate_etag(const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t left = get_size();
  int64_t cur_ofs = 0;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];

  bufferlist etag_bl;

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
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  etag_bl.append(calc_md5, sizeof(calc_md5));
  get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));
  return write_attrs(dpp, y);
}

const std::string POSIXObject::get_fname(bool use_version)
{
  return get_key_fname(state.obj.key, use_version);
}

std::string POSIXObject::gen_temp_fname()
{
  std::string temp_fname;
  enum { RAND_SUFFIX_SIZE = 8 };
  char buf[RAND_SUFFIX_SIZE + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, RAND_SUFFIX_SIZE);
  temp_fname = "." + get_fname(/*use_version=*/true) + ".";
  temp_fname.append(buf);

  return temp_fname;
}

int POSIXObject::POSIXReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int64_t left;
  int64_t cur_ofs = ofs;

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

int POSIXObject::POSIXReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
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

int POSIXObject::POSIXDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y, uint32_t flags)
{
  return source->delete_object(dpp, y, flags, nullptr, nullptr);
}

int POSIXObject::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      POSIXBucket *sb, POSIXBucket *db, POSIXObject *dobj)
{
  rgw_obj_key dst_key = dobj->get_key();
  if (!get_key().instance.empty())
    dst_key.instance = get_key().instance;

  return ent->copy(dpp, y, db->get_dir(), get_key_fname(dst_key, /*use_version=*/true));
}

void POSIXMPObj::init_gen(POSIXDriver* driver, const std::string& _oid, ACLOwner& _owner)
{
  char buf[33];
  std::string new_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
  /* Generate an upload ID */

  gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
  new_id.append(buf);
  init(_oid, new_id, _owner);
}

int POSIXMultipartPart::load(const DoutPrefixProvider* dpp, optional_yield y,
			     POSIXDriver* driver, rgw_obj_key& key)
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

  ret = decode_attr(attrs, RGW_POSIX_ATTR_MPUPLOAD, info);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": failed to decode part info: " << key << dendl;
    return ret;
  }

  return 0;
}

int POSIXMultipartUpload::load(const DoutPrefixProvider *dpp, bool create)
{
  int ret = 0;
  if (!shadow) {
    POSIXBucket* pb = static_cast<POSIXBucket*>(bucket);
    std::optional<std::string> ns{mp_ns};

    std::unique_ptr<Directory> mpdir = std::make_unique<MPDirectory>(bucket_fname(get_meta(), ns), pb->get_dir(), driver->ctx());

    shadow = std::make_unique<POSIXBucket>(driver, std::move(mpdir), rgw_bucket(std::string(), get_meta()), mp_ns);

    ret = shadow->load_bucket(dpp, null_yield);
    if (ret == -ENOENT && create) {
      ret = shadow->create(dpp, null_yield, nullptr);
    }
  }

  return ret;
}

std::unique_ptr<rgw::sal::Object> POSIXMultipartUpload::get_meta_obj()
{
  load(nullptr);
  if (!shadow) {
    // This upload doesn't exist, but the API doesn't check this until it calls
    // on the *serializer*. So make a fake object in the parent bucket that
    // doesn't exist.  Put it in the MP namespace just in case.
    return bucket->get_object(rgw_obj_key(get_meta(), std::string(), mp_ns));
  }
  return shadow->get_object(rgw_obj_key(get_meta(), std::string()));
}

int POSIXMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
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

  ret = static_cast<POSIXObject*>(meta_obj.get())->open(dpp, true);
  if (ret < 0) {
    return ret;
  }

  mp_obj.upload_info.cksum_type = cksum_type;
  mp_obj.upload_info.dest_placement = dest_placement;
  mp_obj.owner = owner;

  bufferlist bl;
  encode(mp_obj, bl);

  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

  return meta_obj->set_obj_attrs(dpp, &attrs, nullptr, y, rgw::sal::FLAG_LOG_OP);
}

int POSIXMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
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
    std::unique_ptr<MultipartPart> part = std::make_unique<POSIXMultipartPart>(this);
    POSIXMultipartPart* ppart = static_cast<POSIXMultipartPart*>(part.get());

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

int POSIXMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
{
  int ret;

  ret = load(dpp);
  if (ret < 0) {
    if (ret == -ENOENT)
      ret = ERR_NO_SUCH_UPLOAD;
    return ret;
  }

  shadow->remove(dpp, true, y);

  return 0;
}

int POSIXMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
				    prefix_map_t& processed_prefixes)
{
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
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
      POSIXMultipartPart* part = static_cast<rgw::sal::POSIXMultipartPart*>(obj_iter->second.get());
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

      ofs += part->get_size();
      accounted_size += part->get_size();
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	   sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)part_etags.size());
  etag = final_etag_str;

  etag_bl.append(etag);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  ret = shadow->merge_and_store_attrs(dpp, attrs, y);
  if (ret < 0) {
    return ret;
  }

  // Rename to target_obj
  ret = shadow->rename(dpp, y, target_obj);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to rename to final name " << target_obj->get_name()
		      << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  POSIXObject *to = static_cast<POSIXObject*>(target_obj);
  POSIXBucket *sb = static_cast<POSIXBucket*>(target_obj->get_bucket());
  if (sb->versioned()) {
    ret = to->set_cur_version(dpp);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int POSIXMultipartUpload::cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
    CephContext *cct, optional_yield y,
    const rgw_obj& obj,
    std::list<rgw_obj_index_key>& remove_objs,
    prefix_map_t& processed_prefixes)
{
  return -ENOTSUP;
}

int POSIXMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y,
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
      ret = decode_attr(meta_obj->get_attrs(), RGW_POSIX_ATTR_MPUPLOAD, mp_obj);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object attrs for mp upload "
	  << get_key() << dendl;
	return ret;
      }
    }
    *rule = &mp_obj.upload_info.dest_placement;
  }

  return 0;
}

std::string POSIXMultipartUpload::get_fname()
{
  std::string name;

  name = "." + mp_ns + "_" + url_encode(get_meta(), true);

  return name;
}

std::unique_ptr<Writer> POSIXMultipartUpload::get_writer(
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

  return std::make_unique<POSIXMultipartWriter>(dpp, y, shadow.get(), part_key,
                                                driver, owner,
                                                ptail_placement_rule, part_num);
}

int POSIXMultipartWriter::prepare(optional_yield y)
{
  int ret = part_file->create(dpp, /*existed=*/nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }

  return part_file->open(dpp);
}

int POSIXMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  return part_file->write(offset, data, dpp, null_yield);
}

int POSIXMultipartWriter::complete(
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
  POSIXUploadPartInfo info;

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
  attrs[RGW_POSIX_ATTR_MPUPLOAD] = bl;

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

int POSIXAtomicWriter::prepare(optional_yield y)
{
  int ret;

  if (obj->versioned()) {
    ret = obj->make_ent(ObjectType::VERSIONED);
  } else {
    ret = obj->make_ent(ObjectType::FILE);
  }
  if (ret < 0) {
    return ret;
  }
  obj->get_obj_attrs(y, dpp);
  obj->close();
  return obj->open(dpp, true, true);
}

int POSIXAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  return obj->write(offset, data, dpp, null_yield);
}

int POSIXAtomicWriter::complete(size_t accounted_size, const std::string& etag,
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

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      // test the object is existing
      if (!obj->check_exists(dpp)) {
	return -ERR_PRECONDITION_FAILED;
      }
    } else {
      bufferlist bl;
      if (!get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (if_nomatch) {
    if (strcmp(if_nomatch, "*") == 0) {
      // test the object is not existing
      if (obj->check_exists(dpp)) {
	return -ERR_PRECONDITION_FAILED;
      }
    } else {
      bufferlist bl;
      if (!get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (strncmp(if_nomatch, bl.c_str(), bl.length()) == 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  bufferlist owner_bl;
  std::unique_ptr<User> user;
  user = driver->get_user(std::get<rgw_user>(owner.id));
  user->load_user(rctx.dpp, rctx.y);
  POSIXOwner po{std::get<rgw_user>(owner.id), user->get_display_name()};
  encode(po, owner_bl);
  attrs[RGW_POSIX_ATTR_OWNER] = owner_bl;

  bufferlist type_bl;

  obj->set_attrs(attrs);
  ret = obj->write_attrs(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: POSIXAtomicWriter failed writing attrs for "
                       << obj->get_name() << dendl;
    return ret;
  }

  ret = obj->link_temp_file(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed writing temp file" << dendl;
    return ret;
  }

  ret = obj->open(dpp);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: POSIXAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = obj->stat(dpp);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: POSIXAtomicWriter failed closing file" << dendl;
    return ret;
  }

  return 0;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newPOSIXDriver(rgw::sal::Driver* next)
{
  rgw::sal::POSIXDriver* driver = new rgw::sal::POSIXDriver(next);

  return driver;
}

}
