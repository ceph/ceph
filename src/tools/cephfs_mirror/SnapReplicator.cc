#include <dirent.h>
#include <string.h>
#include <limits.h>

#include "common/debug.h"
#include "SnapReplicator.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "SnapReplicator " << __func__


using namespace std::literals::string_literals;

bool operator ==(const struct timespec& lhs, const struct timespec& rhs);
bool operator >(const struct timespec& lhs, const struct timespec& rhs);
bool operator >=(const struct timespec& lhs, const struct timespec& rhs);


SnapReplicator::SnapReplicator(struct ceph_mount_info *_src_mnt,
                               struct ceph_mount_info *_dst_mnt,
                               const std::string& _src_dir,
                               const std::pair<std::string, std::string>& snaps)
  : m_src_mnt(_src_mnt),
    m_dst_mnt(_dst_mnt),
    m_src_dir(_src_dir),
    m_old_snap(snaps.first),
    m_new_snap(snaps.second)
{
  char buf[128] = {0,};
  if (ceph_conf_get(m_src_mnt, "client_snapdir", buf, sizeof(buf)) < 0)
    dout(20) << ": unable to get src client_snapdir" << dendl;
  else
    m_src_snap_dir = buf;
  if (ceph_conf_get(m_dst_mnt, "client_snapdir", buf, sizeof(buf)) < 0)
    dout(20) << ": unable to get dst client_snapdir" << dendl;
  else
    m_dst_snap_dir = buf;
}

// IMPORTANT: old_dentry contains path to old dentry inside the old snap
int SnapReplicator::handle_old_entry(const std::string& dir_name,
                                     const std::string& old_dentry,
                                     const struct dirent& de,
                                     const struct ceph_statx& old_stx,
                                     const struct ceph_statx& new_stx,
                                     std::queue<std::string>& dir_queue)
{
  dout(20) << ": processing old entry " << old_dentry << dendl;

  std::string entry_path = dir_name + "/"s + de.d_name;
  if (S_ISDIR(new_stx.stx_mode)) {
    dir_queue.push(entry_path);
  } else if (S_ISREG(new_stx.stx_mode)) {
    if (new_stx.stx_mtime > old_stx.stx_mtime or new_stx.stx_ctime > old_stx.stx_ctime) {
      if (copy_blocks(entry_path, file_type_t::OLD_FILE, old_stx, new_stx) < 0) {
        dout(20) << ":  failed to copy blocks for " << old_dentry << dendl;
        return -1;
      }

      std::string dst_dentry_path = m_src_dir + "/"s + entry_path;
      if (new_stx.stx_size != old_stx.stx_size) {
        if (ceph_truncate(m_dst_mnt, dst_dentry_path.c_str(), new_stx.stx_size) < 0) {
          dout(20) << ":  failed to truncate to " << new_stx.stx_size << dendl;
          return -1;
        }
      }
      if (new_stx.stx_uid != old_stx.stx_uid or new_stx.stx_gid != old_stx.stx_gid) {
        if (ceph_lchown(m_dst_mnt, dst_dentry_path.c_str(), new_stx.stx_uid, new_stx.stx_gid) < 0) {
          dout(20) << ":  failed to lchown(uid:" << new_stx.stx_uid << ", gid:" << new_stx.stx_gid << ")" << dendl;
          return -1;
        }
      }
    }
  } else if (S_ISLNK(new_stx.stx_mode)) {
    // compare old and new symlink targets and relink if not the same
    if (new_stx.stx_mtime > old_stx.stx_mtime or new_stx.stx_ctime > old_stx.stx_ctime) {
      int rv;
      char old_target[PATH_MAX+1];

      rv = ceph_readlink(m_src_mnt, old_dentry.c_str(), old_target, sizeof(old_target));
      if (rv < 0) {
        dout(20) << ": error reading old symlink: " << old_dentry << dendl;
        m_my_errno = -rv;
        return -1;
      }

      // reading from new snap needs relative path
      char new_target[PATH_MAX+1];
      std::string new_symlink = "."s + dir_name + "/"s + de.d_name;

      rv = ceph_readlink(m_src_mnt, new_symlink.c_str(), new_target, sizeof(new_target));
      if (rv < 0) {
        dout(20) << ": error reading new symlink: " << new_symlink << dendl;
        m_my_errno = -rv;
        return -1;
      }

      if (std::string(old_target) != std::string(new_target)) {
        std::string old_symlink = m_src_dir + dir_name + "/"s + de.d_name;
        rv = ceph_unlink(m_dst_mnt, old_symlink.c_str());
        if (rv < 0) {
          dout(20) << ": error unlinking old symlink: " << old_symlink << dendl;
          m_my_errno = -rv;
          return -1;
        }

        rv = ceph_symlink(m_dst_mnt, new_target, old_symlink.c_str());
        if (rv < 0) {
          dout(20) << ": error symlinking old symlink: " << old_symlink << " to: " << new_target << dendl;
          m_my_errno = -rv;
          return -1;
        }
      }
    }
  }
  return 0;
}

int SnapReplicator::handle_new_entry(const std::string& dir_name,
                                     const struct dirent& de,
                                     const struct ceph_statx& new_stx,
                                     std::queue<std::string>& dir_queue)
{
  std::string dentry = dir_name + "/"s + de.d_name;
  std::string dst_entry_path = m_src_dir + dentry;

  int rv = 0;

  dout(20) << ": processing new entry " << dst_entry_path << dendl;
  if (S_ISDIR(new_stx.stx_mode)) {
    rv = ceph_mkdir(m_dst_mnt, dst_entry_path.c_str(), new_stx.stx_mode);
    if (rv == 0 || rv == -EEXIST) {
      dout(20) << ":  created dir " << dst_entry_path << dendl;
      dout(20) << ":  queueing dentry " << dentry << dendl;
      dir_queue.push(dentry);
    } else {
      dout(20) << ":  failed to create dir "
               << dst_entry_path << " (" << strerror(-rv) << ")" << dendl;
      m_my_errno = -rv;
      return -1;
    }
  } else if (S_ISREG(new_stx.stx_mode)) {
    struct ceph_statx old_stx{0,};
    rv = copy_blocks(dentry, file_type_t::NEW_FILE, old_stx, new_stx);
    if (rv < 0) {
      dout(20) << ":  failed to copy blocks for " << dst_entry_path << dendl;
      m_my_errno = -rv;
      return -1;
    }
    rv = ceph_truncate(m_dst_mnt, dst_entry_path.c_str(), new_stx.stx_size);
    if (rv < 0) {
      dout(20) << ":  failed to truncate to " << new_stx.stx_size << dendl;
      m_my_errno = -rv;
      return -1;
    }
    rv = ceph_lchown(m_dst_mnt, dst_entry_path.c_str(), new_stx.stx_uid, new_stx.stx_gid);
    if (rv < 0) {
      dout(20) << ":  failed to lchown(uid:" << new_stx.stx_uid << ", gid:" << new_stx.stx_gid << ")" << dendl;
      m_my_errno = -rv;
      return -1;
    }
  } else if (S_ISLNK(new_stx.stx_mode)) {
    char buf[PATH_MAX+1] = {0,};
    // std::string src_entry_path = m_src_dir + "/"s + m_src_snap_dir + "/"s + m_new_snap + dentry;
    rv = ceph_readlink(m_src_mnt, ("."s + dentry).c_str(), buf, sizeof(buf));
    if (rv <= 0) {
      dout(20) << ":  failed to readlink(" << dentry << ") (" << strerror(-rv) << ")" << dendl;
      m_my_errno = -rv;
      return -1;
    }
    rv = ceph_symlink(m_dst_mnt, buf, dst_entry_path.c_str());
    if (rv < 0) {
      dout(20) << ":  failed to symlink(" << buf << ", " << dst_entry_path << ") (" << strerror(-rv) << ")" << dendl;
      m_my_errno = -rv;
      return -1;
    }
  } else {
    dout(20) << ":  unhandled entry " << de.d_name << dendl;
  }
  return 0;
}

inline mode_t SnapReplicator::file_type(mode_t m)
{
  return (m & __S_IFMT);
}

// delete the tree at the destination
int SnapReplicator::del_tree(const std::string& entry_path)
{
  dout(20) << ": " << entry_path << dendl;

  struct ceph_dir_result *dirp = nullptr;
  int rv = ceph_opendir(m_dst_mnt, entry_path.c_str(), &dirp);

  if (rv < 0) {
    dout(20) << ": opendir(" << entry_path << ") failed" << dendl;
    m_my_errno = -rv;
    return -1;
  }

  ceph_assert(std::string(ceph_readdir(m_dst_mnt, dirp)->d_name) == ".");
  ceph_assert(std::string(ceph_readdir(m_dst_mnt, dirp)->d_name) == "..");

  struct dirent de;
  struct ceph_statx stx = {0, };

  while (ceph_readdirplus_r(m_dst_mnt, dirp, &de, &stx, CEPH_STATX_BASIC_STATS, AT_SYMLINK_NOFOLLOW, NULL) > 0) {
    std::string dst_path = (entry_path + "/"s + de.d_name);
    if (S_ISDIR(stx.stx_mode))
      del_tree(dst_path);
    else {
      dout(20) << ": removing(" << dst_path << ")" << dendl;
      if ((rv = ceph_unlink(m_dst_mnt, dst_path.c_str())) < 0) {
        dout(20) << ": unlink(" << dst_path << ") failed" << dendl;
        m_my_errno = -rv;
        return -1;
      }
    }
  }
  ceph_closedir(m_dst_mnt, dirp);
  dirp = nullptr;

  rv = ceph_rmdir(m_dst_mnt, entry_path.c_str());
  if (rv < 0) {
    m_my_errno = -rv;
    return -1;
  }
  return 0;
}

int SnapReplicator::replicate()
{
  bool is_error = false;
  std::string new_snap_root = m_src_dir + "/"s + m_src_snap_dir + "/"s + m_new_snap;
  struct ceph_statx old_stx;
  struct ceph_statx new_stx;
  struct ceph_dir_result *new_dirp = NULL;
  struct dirent de;
  bool is_first_sync = (m_old_snap == ""s);
  std::queue<std::string> dir_queue;

  if (ceph_chdir(m_src_mnt, new_snap_root.c_str()) < 0) {
    dout(20) << ": unable to chdir to " << new_snap_root << dendl;
    return -1;
  } else {
    dout(20) << ": chdir to " << new_snap_root << dendl;
  }

  dir_queue.push(""s);
  // replicate the changes
  // we traverse the new snap only in dirs with (mtime|ctime) > old snap ctime
  while (!is_error && !dir_queue.empty()) {
    std::string dir_name = dir_queue.front();
    dir_queue.pop();

    if (ceph_opendir(m_src_mnt, ("."s + dir_name).c_str(), &new_dirp) < 0) {
      dout(20) << ": opendir failed for " << ("." + dir_name) << dendl;
      is_error = true;
      break;
    }
    ceph_assert(std::string(ceph_readdir(m_src_mnt, new_dirp)->d_name) == ".");
    ceph_assert(std::string(ceph_readdir(m_src_mnt, new_dirp)->d_name) == "..");

    while (ceph_readdirplus_r(m_src_mnt, new_dirp, &de, &new_stx, CEPH_STATX_BASIC_STATS, AT_SYMLINK_NOFOLLOW, NULL) > 0) {
      if (!is_first_sync) {
        std::string old_dentry = m_src_dir      + "/"s +
                                 m_src_snap_dir + "/"s +
                                 m_old_snap     +
                                 dir_name       +
                                 "/"s + de.d_name;

        int rv = 0;
        rv = ceph_statx(m_src_mnt, old_dentry.c_str(), &old_stx, CEPH_STATX_BASIC_STATS, AT_SYMLINK_NOFOLLOW);
        if (rv == 0) {
          if (file_type(old_stx.stx_mode) != file_type(new_stx.stx_mode)) {
            std::string odentry = m_src_dir + dir_name + "/"s + de.d_name;
            if (S_ISDIR(old_stx.stx_mode)) {
              if (del_tree(odentry) < 0) {
                dout(20) << "error removing old tree: " << odentry << dendl;
                continue;
              }
            } else {
              rv = ceph_unlink(m_dst_mnt, odentry.c_str());
              if (rv < 0) {
                dout(20) << "error removing old entry: " << odentry << dendl;
                continue;
              }
            }

            if (handle_new_entry(dir_name, de, new_stx, dir_queue) < 0) {
              is_error = true;
              break;
            }
          } else {
            // found entry in old snap
            if (handle_old_entry(dir_name, old_dentry, de, old_stx, new_stx, dir_queue) < 0) {
              is_error = true;
              break;
            }
          }
        } else if (rv == -ENOENT || rv == -ENOTDIR) {
          if (handle_new_entry(dir_name, de, new_stx, dir_queue) < 0) {
            is_error = true;
            break;
          }
        } else {
          dout(20) << ": error while getting statx for old entry " << old_dentry << " (" << strerror(-rv) << ")" << dendl;
          is_error = true;
          break;
        }
      } else {
        if (handle_new_entry(dir_name, de, new_stx, dir_queue) < 0) {
          is_error = true;
          break;
        }
      }
    }
    if (ceph_closedir(m_src_mnt, new_dirp) < 0) {
      dout(20) << ": closedir failed for " << ("." + dir_name) << dendl;
    }
    new_dirp = NULL;
  }
  if (!is_error) {
    int rv = 0;
    std::string dst_snap = m_src_dir + "/"s + m_dst_snap_dir + "/"s + m_new_snap;
    if ((rv = ceph_mkdir(m_dst_mnt, dst_snap.c_str(), 0755)) < 0 and rv != -EEXIST) {
      dout(20) << ": failed to create dst snapshot dir " << dst_snap << dendl;
      m_my_errno = -rv;
      return -1;
    }
    return 0;
  }
  return -1;
}

bool SnapReplicator::is_system_dir(const char *dir) const
{
  if ((dir[0] == '.' && dir[1] == '\0') ||
      (dir[0] == '.' && dir[1] == '.' && dir[2] == '\0'))
    return true;

  return false;
}

int SnapReplicator::copy_remaining(int read_fd, int write_fd, off_t read, int len)
{
  bool is_error = false;
  int rv = 0;

  while (read < len) {
    if ((rv = ceph_read(m_src_mnt, read_fd, m_readbuf_new, TXN_BLOCK_SIZE, read)) < 0) {
      dout(20) << ": source file read with ceph_read(" << read_fd << ", " << TXN_BLOCK_SIZE << ", " << read << ") failed" << dendl;
      m_my_errno = -rv;
      is_error = true;
      break;
    }
    int read_bytes = rv;
    if ((rv = ceph_write(m_dst_mnt, write_fd, m_readbuf_new, rv, read)) < 0) {
      dout(20) << ": destination file write with ceph_write(" << write_fd << ", " << read_bytes << ", " << read << ") failed" << dendl;
      m_my_errno = -rv;
      is_error = true;
      break;
    }
    read += rv;
  }
  if (is_error)
    return -1;
  return 0;
}

int SnapReplicator::copy_all_blocks(const std::string& en, const struct ceph_statx& new_stx)
{
  std::string new_root = m_src_dir + "/"s + m_src_snap_dir + "/"s + m_new_snap;
  int rv = ceph_open(m_src_mnt, (new_root + en).data(), O_RDONLY, 0);
  if (rv < 0) {
    dout(20) << ": opening new source file with ceph_open(" << (new_root + en) << ", O_RDONLY) failed" << dendl;
    m_my_errno = -rv;
    return -1;
  }
  int read_fd = rv;

  rv = ceph_open(m_dst_mnt, (m_src_dir + en).c_str(), O_CREAT|O_WRONLY, new_stx.stx_mode);
  if (rv < 0) {
    dout(20) << ": opening new destination file with ceph_open(" << (en) << ", O_CREAT) failed" << dendl;
    m_my_errno = -rv;
    ceph_close(m_src_mnt, read_fd);
    return -1;
  }
  int write_fd = rv;

  const off_t len = new_stx.stx_size;

  copy_remaining(read_fd, write_fd, 0, len);
  ceph_close(m_dst_mnt, write_fd);
  ceph_close(m_src_mnt, read_fd);
  return 0;
}

int SnapReplicator::copy_blocks(const std::string& en, file_type_t ftype,
                                const struct ceph_statx& old_stx,
                                const struct ceph_statx& new_stx)
{
  bool is_error = false;

  if (ftype == file_type_t::OLD_FILE) {
    off_t old_size = old_stx.stx_size;

    if (old_size == 0) {
      dout(20) << ": old file size is zero; copying all new file blocks" << dendl;
      copy_all_blocks(en, new_stx);
    }

    std::string old_root = m_src_dir + "/"s + m_src_snap_dir + "/"s + m_old_snap;

    int rv = ceph_open(m_src_mnt, (old_root + en).c_str(), O_RDONLY, 0);

    if (rv < 0) {
      dout(20) << ": ceph_open(" << (old_root + en) << ") failed" << dendl;
      m_my_errno = -rv;
      return -1;
    }

    int read_fd_old = rv;
    std::string new_root = m_src_dir + "/"s + m_src_snap_dir + "/"s + m_new_snap;
    rv = ceph_open(m_src_mnt, (new_root + en).c_str(), O_RDONLY, 0);
    if (rv < 0) {
      dout(20) << ": ceph_open(" << (new_root + en) << ") failed" << dendl;
      m_my_errno = -rv;
      return -1;
    }
    int read_fd_new = rv;

    off_t read = 0;
    const off_t new_size = new_stx.stx_size;

    if (new_size == 0) {
      dout(20) << ": new file size is zero; nothing to copy" << dendl;
      return 0;
    }

    dout(20) << ": starting copy of file blocks" << dendl;
    if (new_size == 0) {
      dout(20) << ": new file size is zero" << dendl;
      return 0;
    }

    rv = ceph_open(m_dst_mnt, (m_src_dir + en).c_str(), O_CREAT|O_WRONLY, new_stx.stx_mode);
    if (rv < 0) {
      m_my_errno = -rv;
      return -1;
    }

    int write_fd = rv;
    const off_t min_size = std::min(old_size, new_size);

    read = 0;
    while (read < min_size) {
      int rv_old = ceph_read(m_src_mnt, read_fd_old, m_readbuf_old, TXN_BLOCK_SIZE, read);

      if (rv_old < 0) {
        dout(20) << ": failed while reading from old file" << dendl;
        m_my_errno = -rv_old;
        is_error = true;
        break;
      }

      int rv_new = ceph_read(m_src_mnt, read_fd_new, m_readbuf_new, TXN_BLOCK_SIZE, read);

      if (rv_new < 0) {
        dout(20) << ": failed while reading from new file" << dendl;
        m_my_errno = -rv_new;
        is_error = true;
        break;
      }

      if (rv_old != rv_new || memcmp(m_readbuf_old, m_readbuf_new, TXN_BLOCK_SIZE) != 0) {
        rv = ceph_write(m_dst_mnt, write_fd, m_readbuf_new, rv_new, read);
        if (rv < 0) {
          dout(20) << ": failed while writing to new file" << dendl;
          m_my_errno = -rv;
          is_error = true;
          break;
        }
      }

      read += rv_new;
    }

    if (!is_error && min_size < new_size) {
      // copy over the remaining suffix chunk to the destination
      if (copy_remaining(read_fd_new, write_fd, read, new_size) < 0)
        is_error = true;
    }

    if (write_fd)
      ceph_close(m_dst_mnt, write_fd);
    if (read_fd_new)
      ceph_close(m_src_mnt, read_fd_new);
    if (read_fd_old)
      ceph_close(m_src_mnt, read_fd_old);

  } else if (ftype == file_type_t::NEW_FILE) {
    if (copy_all_blocks(en, new_stx) < 0)
      return -1;
  }
  if (is_error)
    return -1;

  return 0;
}

bool operator ==(const struct timespec& lhs, const struct timespec& rhs)
{
  return ((lhs.tv_sec == rhs.tv_sec) && (lhs.tv_nsec == rhs.tv_nsec));
}

bool operator >(const struct timespec& lhs, const struct timespec& rhs)
{
  if (lhs.tv_sec > rhs.tv_sec)
    return true;

  if (lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec > rhs.tv_nsec)
    return true;

  return false;
}

bool operator >=(const struct timespec& lhs, const struct timespec& rhs)
{
  return ((lhs == rhs) or (lhs > rhs));
}
