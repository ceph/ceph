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
#include "common/errno.h"
#include "include/random.h"
#include "driver/posix/sync_policy.h"
#include "../posix/posix_io_uring.h"

namespace rgw { namespace sal {

const std::string NSFS_FOLDER_OBJECT_NAME = ".folder";

int delete_directory(int parent_fd, const char* dname, bool delete_children,
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
  if (get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
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

  direct_io = ctx->_conf.get_val<bool>("rgw_nsfs_direct_io");
  if (direct_io) {
    flags |= O_DIRECT;
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

  direct_io = ctx->_conf.get_val<bool>("rgw_nsfs_direct_io");
  int flags = O_RDWR | (direct_io ? O_DIRECT : 0);

  int ret = openat(parent->get_fd(), fname.c_str(), flags, S_IRWXU);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  fd = ret;

  if (!direct_io) {
    /* fadvise only tunes buffered-read readahead; O_DIRECT bypasses the
     * page cache entirely, so the hint would be a no-op syscall. */
    int read_fadvise = ctx->_conf.get_val<int64_t>("rgw_nsfs_read_fadvise");
    if (read_fadvise != POSIX_FADV_NORMAL) {
      ::posix_fadvise(fd, 0, 0, read_fadvise);
    }
  }

  return 0;
}

int File::close()
{
  if (fd < 0) {
    return 0;
  }

  if (need_fsync) {
    auto policy = rgw::posix::parse_sync_policy(
      ctx->_conf.get_val<std::string>("rgw_posix_sync_policy"));
    if (policy == rgw::posix::SyncPolicy::ALWAYS ||
        policy == rgw::posix::SyncPolicy::COMPLETE) {
      int ret = ::fdatasync(fd);
      if (ret < 0) {
        return ret;
      }
    }
    int write_fadvise = ctx->_conf.get_val<int64_t>("rgw_nsfs_write_fadvise");
    if (write_fadvise != POSIX_FADV_NORMAL) {
      ::posix_fadvise(fd, 0, 0, write_fadvise);
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
  int64_t write_chunk_size =
    ctx->_conf.get_val<Option::size_t>("rgw_nsfs_write_chunk_size");
  int64_t left = bl.length();
  ssize_t ret;

  ret = fchmod(fd, S_IRUSR|S_IWUSR);
  if(ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not change permissions on object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (direct_io) {
    /* Never fcntl-off O_DIRECT on a shared fd; pad or RMW instead. */
    ret = posix_direct_write(fd, ofs, bl, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "URING: ERROR: File::write posix_direct_write failed: "
                        << cpp_strerror(-ret) << " (" << ret << ")" << dendl;
    }
    return ret;
  }

  char* curp = bl.c_str();
  const bool positioned =
    ctx->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth") >= 2;
  int64_t woff = ofs;

  if (!positioned) {
    ret = lseek(fd, ofs, SEEK_SET);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
        << ofs << " :" << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  while (left > 0) {
    int64_t want = (write_chunk_size > 0) ? std::min(left, write_chunk_size) : left;
    if (positioned) {
      ret = ::pwrite(fd, curp, want, woff);
    } else {
      ret = ::write(fd, curp, want);
    }
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not write object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    curp += ret;
    woff += ret;
    left -= ret;
  }

  return 0;
}

int File::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  int64_t read_chunk_size =
    ctx->_conf.get_val<Option::size_t>("rgw_nsfs_read_chunk_size");
  if (read_chunk_size <= 0) {
    read_chunk_size = READ_SIZE;
  }
  int64_t len = std::min(left, read_chunk_size);
  ssize_t ret;

  if (direct_io) {
    /* O_DIRECT requires the read offset, length, and buffer address to be
     * aligned to the filesystem block size.  Round the requested window
     * out to DIRECT_IO_ALIGN and slice the exact bytes back out of the
     * aligned buffer, so callers can keep passing arbitrary ranges. */
    int64_t aligned_ofs = ofs & ~(DIRECT_IO_ALIGN - 1);
    int64_t front = ofs - aligned_ofs;
    int64_t aligned_len = ((front + len + DIRECT_IO_ALIGN - 1) /
                           DIRECT_IO_ALIGN) * DIRECT_IO_ALIGN;

    bufferptr bp(buffer::create_small_page_aligned(aligned_len));
    ret = ::pread(fd, bp.c_str(), aligned_len, aligned_ofs);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    int64_t got = std::min<int64_t>(std::max<int64_t>(ret - front, 0), len);
    if (got > 0) {
      bl.append(bp, front, got);
    }
    return got;
  }

  bufferptr bp(len);
  if (ctx->_conf.get_val<uint64_t>("rgw_nsfs_get_iodepth") >= 2) {
    ret = ::pread(fd, bp.c_str(), len, ofs);
  } else {
    ret = lseek(fd, ofs, SEEK_SET);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
                        << ofs << " :" << cpp_strerror(ret) << dendl;
      return -ret;
    }
    ret = ::read(fd, bp.c_str(), len);
  }
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  bl.append(bp, 0, ret);

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
      if (get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
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
              /* FLAG_CURRENT for orphaned DMs (no top-level file) is
               * resolved by a fixup pass over LMDB after fill_cache
               * completes — see bucket_cache.h fixup_current_flags() */
            }

            Attrs attrs;
            ret = get_x_attrs(y, dpp, tfd, attrs, vname);

            ACLOwner acl_owner;
            if (decode_acl_owner(attrs, acl_owner) >= 0) {
              bde.meta.owner = to_string(acl_owner.id);
              bde.meta.owner_display_name = acl_owner.display_name;
            }

            bufferlist etag_bl;
            if (get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
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
	ldpp_dout(dpp, 0) << "ERROR: could not create multipart directory " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }


  ret = openat(parent->get_fd(), path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not open multipart directory " << get_name()
                      << dendl;
    return ret;
  }

  fd = ret;
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

} } // namespace rgw::sal
