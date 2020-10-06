#ifndef SNAP_REPLICATOR_H
#define SNAP_REPLICATOR_H

#include <string>
#include <queue>
#include <utility>

#include "common/ceph_context.h"
#include "include/cephfs/libcephfs.h"


enum file_type_t {
  OLD_FILE = 1,
  NEW_FILE = 2
};

#define TXN_BLOCK_SIZE 65536

class SnapReplicator {
public:
  SnapReplicator(struct ceph_mount_info *_src_mnt,
                 struct ceph_mount_info *_dst_mnt,
                 const std::string& src_dir,
                 const std::pair<std::string, std::string>& snaps);

  ~SnapReplicator() {
    delete [] m_readbuf_old;
    delete [] m_readbuf_new;
  }

  int replicate();

private:
  struct ceph_mount_info *m_src_mnt;
  struct ceph_mount_info *m_dst_mnt;
  std::string m_src_dir;
  std::string m_src_snap_dir; // eg. ".snap"
  std::string m_dst_snap_dir; // eg. ".snap"
  std::string m_old_snap;
  std::string m_new_snap;

  int m_my_errno = 0;
  char *m_readbuf_old = new char[TXN_BLOCK_SIZE];
  char *m_readbuf_new = new char[TXN_BLOCK_SIZE];
  
  bool is_system_dir(const char *dir) const;
  int copy_remaining(int read_fd, int write_fd, off_t read, int len);
  int copy_all_blocks(const std::string& en, const struct ceph_statx& new_stx);
  int copy_blocks(const std::string& en, file_type_t ftype,
                  const struct ceph_statx& old_stx,
                  const struct ceph_statx& new_stx);
  int handle_old_entry(const std::string& dir_name,
                       const std::string& old_dentry,
                       const struct dirent& de,
                       const struct ceph_statx& old_stx,
                       const struct ceph_statx& new_stx,
                       std::queue<std::string>& dir_queue);
  int handle_new_entry(const std::string& dir_name,
                       const struct dirent& de,
                       const struct ceph_statx& new_stx,
                       std::queue<std::string>& dir_queue);
  inline mode_t file_type(mode_t m);
  int del_tree(const std::string& entry_path);
};

#endif // SNAP_REPLICATOR_H
