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

#include "fsent.h"
#include <dirent.h>
#include "include/random.h"

namespace rgw { namespace sal {

const std::string ATTR_PREFIX = "user.X-RGW-";
const std::string mp_ns = "multipart";
const std::string MP_OBJ_PART_PFX = "part-";
const std::string MP_OBJ_HEAD_NAME = MP_OBJ_PART_PFX + "00000";
const int64_t READ_SIZE = 128 * 1024;

namespace posix {

/*
 * Object ownership is stored in RGW_ATTR_ACL (the standard ACL policy
 * attribute written by the generic RGW layer), matching the rados driver.
 * Earlier code maintained a separate "POSIX-Owner" xattr with a
 * POSIXOwner struct that only held rgw_user — this could not represent
 * account-owned objects (STS role sessions, account root users) and
 * crashed with std::bad_variant_access.  Removed in favour of the
 * generic ACLOwner which carries the full rgw_owner variant.
 */


/* RAII guard for OFD (Open File Description) locks.  OFD locks are
 * per-open-file-description rather than per-process, so they work
 * correctly across threads that open independent fds to the same
 * inode.  Used to serialise xattr writes against concurrent readers
 * on the same directory or file. */
struct OFDLockGuard {
  int fd;
  OFDLockGuard(int _fd, short type) : fd(_fd) {
    struct flock fl{};
    fl.l_type = type;
    fl.l_whence = SEEK_SET;
    fcntl(fd, F_OFD_SETLKW, &fl);
  }
  ~OFDLockGuard() {
    struct flock fl{};
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    fcntl(fd, F_OFD_SETLK, &fl);
  }
};

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

  attrname = ATTR_PREFIX + key;

  ret = fsetxattr(fd, attrname.c_str(), value.c_str(), value.length(), 0);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not write attribute " << attrname << " for " << display << ": " << cpp_strerror(ret) << dendl;
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

  OFDLockGuard lock(fd, F_WRLCK);
  need_fsync = true;

  /* Set the type */
  bufferlist type_bl;
  ObjectType type{get_type()};
  type.encode(type_bl);
  attrs[RGW_POSIX_ATTR_OBJECT_TYPE] = type_bl;

  /* Snapshot old xattrs so we can remove genuinely stale ones after
   * writing — covered by the OFD write lock above. */
  Attrs old_attrs;
  int old_ret = get_x_attrs(y, dpp, fd, old_attrs, get_name());

  /* Write new values first — fsetxattr overwrites in place, so
   * readers always see either the old or the new value, never
   * ENODATA for an attr that is being updated. */
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

  /* Now remove xattrs that are on disk but genuinely gone from the
   * new attrs (not just about to be rewritten). */
  if (old_ret >= 0) {
    for (auto& it : old_attrs) {
      if (attrs.find(it.first) == attrs.end() &&
          (!extra_attrs || extra_attrs->find(it.first) == extra_attrs->end())) {
        remove_x_attr(dpp, y, fd, it.first, get_name());
      }
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

  OFDLockGuard lock(get_fd(), F_RDLCK);
  return get_x_attrs(y, dpp, get_fd(), attrs, get_name());
}

int FSEnt::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags)
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
      if (flags & FSEnt::FLAG_CURRENT) {
	  bde.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
      }
      if (flags & FSEnt::FLAG_DELETE_MARKER) {
        bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
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
  if (get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
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
      ret = del->remove(dpp, y, /*delete_children=*/true, nullptr);
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

int File::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result)
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

  /* note that open() and stat() return already sign-reversed result codes */
  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: POSIXAtomicWriter failed closing file" << dendl;
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

int Directory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result)
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
      ret = del->remove(dpp, y, /*delete_children=*/true, nullptr);
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
    if (tmpfd >= 0) {
      ret = get_x_attrs(y, dpp, tmpfd, attrs, name);
      if (ret >= 0) {
        decode_attr(attrs, RGW_POSIX_ATTR_OBJECT_TYPE, type);
      }
      ::close(tmpfd);
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
                          fill_cache_cb_t &cb, uint32_t flags)
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

    ret = ent->fill_cache(dpp, y, cb, FSEnt::FLAG_NONE);
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

int MPDirectory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result)
{
  return Directory::remove(dpp, y, /*delete_children=*/true, result);
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
                            fill_cache_cb_t &cb, uint32_t flags)
{
  int ret = FSEnt::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE);
  if (ret < 0)
    return ret;

  return Directory::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE);
}

int VersionedDirectory::open(const DoutPrefixProvider* dpp)
{
  if (fd > 0) {
    return 0;
  }
  int ret = Directory::open(dpp);
  if (ret < 0) {
    return ret;
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
    ret = del->remove(dpp, null_yield, /*delete_children=*/true, nullptr);
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
    return 0;
  }
  ret = cur_version->stat(dpp);
  if (ret < 0)
    return ret;
  stx.stx_size = cur_version->get_stx().stx_size;

  if (cur_version->get_stx().stx_size == 0) {
    //Possibly a delete marker
    Attrs attrs;
    ret = cur_version->read_attrs(dpp, null_yield, attrs);
    if (ret < 0) {
      return ret;
    }
    bufferlist bl;
    if (get_attr(attrs, RGW_POSIX_ATTR_VERSION, bl)) {
      uint16_t flags = 0;
      ceph::decode(flags, bl);
      if (flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
        ldpp_dout(dpp, 0) << "ERROR: a delete marker, returning ENOENT "
                          << get_name() << dendl;
        cur_version.reset();
        return -ENOENT;
      }
    }
  }

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
      ret = del->remove(dpp, y, /*delete_children=*/true, nullptr);
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

int VersionedDirectory::add_delete_marker(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::unique_ptr<File>& marker,
                                          const std::string &name)
{
  // Create as temporary file first
  int ret = marker->create(dpp, /*existed=*/nullptr, /*temp_file=*/true);
  if (ret < 0) {
    return ret;
  }

  // XXX: Hack to set the owner on the delete marker
  Attrs v_attrs;
  Attrs attrs;

  ret = get_x_attrs(y, dpp, get_fd(), v_attrs, get_name());
  if (ret < 0) {
    // removing the temporary files before returning failure
    marker->remove(dpp, y, /*delete_children=*/false, nullptr);
    return ret;
  }

  bufferlist owner_bl;
  if (get_attr(v_attrs, RGW_ATTR_ACL, owner_bl)) {
    attrs[RGW_ATTR_ACL] = std::move(owner_bl);
  }

  buffer::list bl;
  uint16_t flags = 0;
  flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
  ceph::encode(flags, bl);
  attrs[RGW_POSIX_ATTR_VERSION] = std::move(bl);

  // Write attributes before linking
  ret = marker->write_attrs(dpp, y, attrs, nullptr);
  if (ret < 0) {
    // removing the temporary files before returning failure
    marker->remove(dpp, y, /*delete_children=*/false, nullptr);
    return ret;
  }

  // Link temp file to final name atomically
  ret = marker->link_temp_file(dpp, y, name);
  if (ret < 0) {
    // removing the temporary files before returning failure
    marker->remove(dpp, y, /*delete_children=*/false, nullptr);
    return ret;
  }

  return 0;
}

int VersionedDirectory::remove(const DoutPrefixProvider* dpp, optional_yield y,
                               bool delete_children, DeleteResult* result)
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
      return Directory::remove(dpp, y, /*delete_children=*/true, result);
    }

    /* Add a delete marker */
    std::unique_ptr<File> f;
    rgw_obj_key key = decode_obj_key(get_name());
    key.instance = gen_rand_instance_name();
    tgtname = get_key_fname(key, /*use_version=*/true);

    result->delete_marker = true;
    result->version_id = key.instance;

    f = std::make_unique<File>(tgtname, this, ctx);
    ret = add_delete_marker(dpp, y, f, tgtname);
    if (ret < 0) {
      return ret;
    }

    newlink = true;
    ret = set_cur_version_ent(dpp, f.get());
    if (ret < 0) {
      return ret;
    }
    cur_version = std::move(f);
    return 0;
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
      Attrs attrs;
      ret = f->read_attrs(dpp, y, attrs);
      if (ret < 0) {
        return ret;
      }
      bufferlist bl;
      if (get_attr(attrs, RGW_POSIX_ATTR_VERSION, bl)) {
       result->delete_marker = true;
      }
      ret = f->remove(dpp, y, /*delete_children=*/true, result);
      if (ret < 0)
       return ret;
      result->version_id = instance_id;
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
      return Directory::remove(dpp, y, /*delete_children=*/true, result);
    }
  }
  if (newlink) {
    exist = true;
    std::unique_ptr<FSEnt> f;
    ret = get_ent(dpp, y, tgtname, std::string(), f);
    if (ret < 0) {
      return ret;
    }
    ret = set_cur_version_ent(dpp, f.get());
    if (ret < 0) {
      return ret;
    }
    cur_version = std::move(f);
  }

  return 0;
}

int VersionedDirectory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                                   fill_cache_cb_t &cb, uint32_t flags)
{
  /* Fill cur_version — stat() may reset cur_version to null if the
   * current version is a delete marker, so also resolve the symlink
   * target name for the FLAG_CURRENT comparison below */
  stat(dpp, /*force=*/false);

  std::string cur_version_name;
  if (cur_version) {
    cur_version_name = cur_version->get_name();
  } else {
    /* cur_version is null — likely a delete marker; read the symlink
     * to determine which entry is current */
    std::unique_ptr<Symlink> sl = std::make_unique<Symlink>(get_name(), this, ctx);
    if (sl->stat(dpp) >= 0 && sl->exists()) {
      cur_version_name = sl->get_target()->get_name();
    }
  }

  int ret = for_each(dpp, [this, &cb, &dpp, &y, &cur_version_name](const char *name) {
    std::unique_ptr<FSEnt> ent;

    if (name[0] == '.') {
      /* Skip dotfiles */
      return 0;
    }

    int ret = get_ent(dpp, y, name, std::string(), ent);
    if (ret < 0)
      return ret;

    ent->stat(dpp); // Stat the object to get the type

    if (ent->get_type() != ObjectType::SYMLINK) {
      uint32_t fill_flags =
          (!cur_version_name.empty() &&
           (ent->get_name() == cur_version_name)) ?
        FSEnt::FLAG_CURRENT :
        FSEnt::FLAG_NONE;

      // Delete markers are zero byte files
      if (ent->get_stx().stx_size == 0) {
        Attrs attrs;
        bufferlist bl;
        ret = ent->read_attrs(dpp, y, attrs);
        if (ret < 0) {
          return ret;
        }
        if (get_attr(attrs, RGW_POSIX_ATTR_VERSION, bl)) {
          fill_flags |= FSEnt::FLAG_DELETE_MARKER;
        }
      }

      ret = ent->fill_cache(dpp, y, cb, fill_flags);
      if (ret < 0)
        return ret;
    }
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

  ret = sl->remove(dpp, y, /*delete_children=*/false, nullptr);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

} // namespace posix

} } // namespace rgw::sal
