#ifndef CEPH_SNAP_DIFF_H
#define CEPH_SNAP_DIFF_H

#include <string>
#include <utility>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <openssl/evp.h>

#include "common/ceph_context.h"
#include "include/cephfs/libcephfs.h"
#include "RollingChecksum.h"

enum snap_cluster_type_t {
  SOURCE = 1,
  DEST   = 2
};

enum snap_type_t {
  NO_SNAP  = -1,
  OLD_SNAP = 0,
  NEW_SNAP = 1
};

enum presence_t {
  PRESENT_IN_NONE = 0, // this can't ever be!
  PRESENT_IN_OLD  = 1,
  PRESENT_IN_NEW  = 2,
  PRESENT_IN_BOTH = 3
};

enum file_type_t {
  OLD_FILE = 1,
  NEW_FILE = 2
};

// prorotypes
bool operator ==(struct timespec const& a, struct timespec const& b);
std::pair<std::string, std::string> get_dir_and_entry(std::string const& entry_path);


class entry_info_t
{
  private:
  ino_t ino;     // inode number
  struct stat st;   // stat info
  std::unordered_set<std::string> entry_names; // starting at snapshot home dir; not at root
  std::unordered_map<std::string, std::string> xattrs; // map from xattr name -> xattr value
  std::string linked_file;  // actual file pointed to by symlink
  bool is_stat_processed = false;
  bool is_name_processed = false;
  bool is_xattr_processed = false;
  bool is_symlink_processed = false;

  public:
  entry_info_t(ino_t _ino, struct stat _st) : ino(_ino), st(_st) {}

  ino_t get_ino() const
  {
    return ino;
  }

  void add_xattr_entry(std::string& xattr_name, void *xattr_value, size_t xattr_value_size);

  std::unordered_set<std::string> const& get_entry_names() const
  {
    return entry_names;
  }

  bool has_entry(std::string const& other_name) const
  {
    return (entry_names.find(other_name) != entry_names.end());
  }

  void add_entry(std::string const& entry)
  {
    entry_names.insert(entry);
  }

  void set_linked_file(std::string const& linked_file)
  {
    this->linked_file = linked_file;
  }

  std::unordered_map<std::string, std::string> const& get_xattrs() const;
  bool has_xattr(std::string const& xattr_name) const;
  bool is_xattr_value_same(std::string const& xattr_name, std::string const& xattr_value) const;

  uid_t get_uid() const
  {
    return st.st_uid;
  }

  bool has_same_uid(uid_t const other) const
  {
    return (st.st_uid == other);
  }

  gid_t get_gid() const
  {
    return st.st_gid;
  }

  bool has_same_gid(gid_t const other) const
  {
    return (st.st_gid == other);
  }

  mode_t get_perms() const
  {
    return (st.st_mode & 0777);
  }

  bool has_same_perms(mode_t const other) const
  {
    mode_t my_perms    = (st.st_mode & 0777);
    mode_t other_perms = (other & 0777);

    return (my_perms == other_perms);
  }

  uint64_t get_size() const
  {
    return st.st_size;
  }

  bool has_same_size(off_t const other) const
  {
    return (st.st_size == other);
  }

  struct timespec get_ctime() const
  {
    return st.st_ctim;
  }

  struct timespec get_atime() const
  {
    return st.st_atim;
  }

  bool has_same_atime(struct timespec const other) const
  {
    return (st.st_atim == other);
  }

  struct timespec get_mtime() const
  {
    return st.st_mtim;
  }

  bool has_same_mtime(struct timespec const other) const
  {
    return (st.st_mtim == other);
  }

  bool is_reg() const
  {
    return S_ISREG(get_type());
  }

  bool is_dir() const
  {
    return S_ISDIR(get_type());
  }

  bool is_chr() const
  {
    return S_ISCHR(get_type());
  }

  bool is_blk() const
  {
    return S_ISBLK(get_type());
  }

  bool is_fifo() const
  {
    return S_ISFIFO(get_type());
  }

  bool is_lnk() const
  {
    return S_ISLNK(get_type());
  }

  bool is_sock() const
  {
    return S_ISSOCK(get_type());
  }

  mode_t get_type() const
  {
    return (st.st_mode & S_IFMT);
  }

  bool is_same_type(mode_t const other) const
  { 
    mode_t my_type = (st.st_mode & S_IFMT);
    mode_t other_type = (other & S_IFMT);

    return (my_type == other_type);
  }

  bool is_same_file_symlinked(std::string const& other) const
  {
    return (linked_file == other);
  }

  std::string const& get_linked_file() const
  {
    return linked_file;
  }

  bool does_entry_exist(std::string const& entry) const
  {
    return (entry_names.find(entry) != entry_names.end());
  }

  bool operator ==(entry_info_t const& rhs) const
  {
    return has_same_uid(rhs.st.st_uid) &&
      has_same_gid(rhs.st.st_gid) &&
      has_same_size(rhs.st.st_size) &&
      has_same_mtime(rhs.st.st_mtim) &&
      is_same_type(rhs.st.st_mode);
  }
};

std::string::size_type count_char(std::string const& str, char c);

struct path_comparator_t {
  // shallow paths compare as lesser than deeper paths
  bool operator ()(std::string const& lhs, std::string const& rhs) const
  {
    std::string::size_type lhs_count = count_char(lhs, '/');
    std::string::size_type rhs_count = count_char(rhs, '/');

    if (lhs_count < rhs_count)
      return true;

    if (lhs_count == rhs_count)
      return (lhs.compare(rhs) < 0);

    return false;
  }
};

// map from inode number -> entry info
typedef std::unordered_map<ino_t, entry_info_t*> ino_entry_map_t;
typedef std::unordered_map<std::string, ino_t> entry_ino_map_t;
typedef std::map<std::string, std::unordered_set<std::string>, path_comparator_t> dir_entries_map_t;

class snap_infos_t
{
  private:
  ino_entry_map_t old_iem;
  ino_entry_map_t new_iem;

  /* following items hold the entire path of an entry for respective snaps */
  entry_ino_map_t old_snap_entries;
  entry_ino_map_t new_snap_entries;

  /* following items hold the list of entries immediately under the dir and
   * not in sub-dirs
   */
  dir_entries_map_t old_snap_dir_entries;
  dir_entries_map_t new_snap_dir_entries;

  public:
  ~snap_infos_t() {
    for (auto& it: old_iem)
      delete it.second;
    for (auto& it: new_iem)
      delete it.second;
  }

  presence_t get_entry_presence(std::string const& entry) const
  {
    auto const& oit = old_snap_entries.find(entry);
    auto const& nit = new_snap_entries.find(entry);
    bool found_in_old_snap = (oit != old_snap_entries.end());
    bool found_in_new_snap = (nit != new_snap_entries.end());

    if (found_in_old_snap) {
      if (found_in_new_snap)
        return presence_t::PRESENT_IN_BOTH;
      return presence_t::PRESENT_IN_OLD;
    } else {
      if (found_in_new_snap)
        return presence_t::PRESENT_IN_NEW;
      return presence_t::PRESENT_IN_NONE;
    }
  }

  ino_t get_ino(std::string const& entry, snap_type_t snap_type) const
  {
    ceph_assert(snap_type == snap_type_t::OLD_SNAP ||
                snap_type == snap_type_t::NEW_SNAP);

    if (snap_type == snap_type_t::OLD_SNAP) {
      auto const& it = old_snap_entries.find(entry);
      if (it != old_snap_entries.end())
        return it->second;
      return 0;
    } else {
      auto const& it = new_snap_entries.find(entry);
      if (it != new_snap_entries.end())
        return it->second;
      return 0;
    }
  }

  ino_entry_map_t const& get_ino_entry_map(snap_type_t type) const
  {
    if (type == snap_type_t::OLD_SNAP)
      return old_iem;
    else
      return new_iem;
  }

  bool does_old_snap_have_entry(std::string const& entry) const
  {
    ino_t ino = get_ino(entry, snap_type_t::OLD_SNAP);
    auto const& it = old_iem.find(ino);

    if (it != old_iem.end()) {
      if (it->second->does_entry_exist(entry))
        return true;
    }
    return false;
  }

  entry_ino_map_t const& get_entries(snap_type_t snap_type) const
  {
    if (snap_type == snap_type_t::OLD_SNAP)
      return old_snap_entries;
    else
      return new_snap_entries;
  }

  // entry_path starts under the snapshot dir with a '/'
  void add_entry(snap_type_t type, uint64_t ino, std::string const& entry_path, struct stat& st, std::string const& linked_file)
  {
    ino_entry_map_t& sim = (type == snap_type_t::OLD_SNAP ? old_iem : new_iem);

    if (sim.find(ino) == sim.end()) {
      sim[ino] = new entry_info_t(ino, st);
    }

    // entry_path starts at snapshot root with a '/'
    ceph_assert(sim[ino]->does_entry_exist(entry_path) == false);
    sim[ino]->add_entry(entry_path);
    if (sim[ino]->is_lnk()) {
      sim[ino]->set_linked_file(linked_file);
    }

    entry_ino_map_t& snap_entries = (type == snap_type_t::OLD_SNAP ? old_snap_entries : new_snap_entries);
    dir_entries_map_t& dir_entries = (type == snap_type_t::OLD_SNAP ? old_snap_dir_entries : new_snap_dir_entries);

    snap_entries[entry_path] = ino;
    auto [dir, entry] = get_dir_and_entry(entry_path);
    std::clog << "adding dir: '" << dir << "', entry: '" << entry << "'\n";
    dir_entries[dir].insert(entry);
  }

  dir_entries_map_t const& get_dentries_map(snap_type_t snap_type) const
  {
    return (snap_type == snap_type_t::OLD_SNAP ? old_snap_dir_entries : new_snap_dir_entries);
  }

};

class ChecksumDataStore
{
  private:
    /* if the weak checksum clashes, then we'll have multiple strong checksums
     * against it; one of them better match to guarantee a real block match
     */
    std::unordered_map<off_t, std::pair<uint32_t, std::string>> checksums;

  public:
    bool does_match_weak(off_t offset, uint32_t weak_csum) const
    {
      auto it = checksums.find(offset);
      if (it != checksums.end())
        return (it->second.first == weak_csum);
      return false;
    }

    bool does_match_strong(off_t offset, uint32_t weak_csum, std::string const& md4_hex_digest) const
    {
      auto const& it = checksums.find(offset);

      if (it != checksums.end()) {
        ceph_assert(it->second.first == weak_csum);
        return (it->second.second == md4_hex_digest);
      }

      return false;
    }

    void add_checksum(off_t offset, uint32_t weak_csum, std::string const& md4_hex_digest)
    {
      ceph_assert(checksums.find(offset) == checksums.end());

      checksums[offset] = std::make_pair(weak_csum, md4_hex_digest);
    }
};

class SnapReplicator
{
  public:
    SnapReplicator(std::string const& _source_conf_path,
                   std::string const& _dest_conf_path,
                   std::string const& _source_keyring_path,
                   std::string const& _dest_keyring_path,
                   std::string const& _source_dir,
                   std::string const& _source_fs_name,
                   std::string const& _dest_fs_name,
                   std::string const& _old_snap_name,
                   std::string const& _new_snap_name,
                   std::string const& _source_auth_id,
                   std::string const& _dest_auth_id) :
      source_conf_path(_source_conf_path),
      dest_conf_path(_dest_conf_path),
      source_keyring_path(_source_keyring_path),
      dest_keyring_path(_dest_keyring_path),
      source_dir(_source_dir),
      source_fs_name(_source_fs_name),
      dest_fs_name(_dest_fs_name),
      old_snap_name(_old_snap_name),
      new_snap_name(_new_snap_name),
      source_auth_id(_source_auth_id),
      dest_auth_id(_dest_auth_id)
    { }

    ~SnapReplicator()
    {
      finish_replication();
    }

    int get_errno() const { return my_errno; }
    int connect_to_cluster(snap_cluster_type_t sct);
    int collect_snap_info(snap_type_t snap_type, struct timespec old_snapshot_ctime);
    int prepare_to_replicate();
    // phase1 for detecting deleted dentries and deleting them from destination
    int replicate_phase1();
    // phase2 for actually copying block updates to old files and new files
    int replicate_phase2(std::string const& restart_after_entry);
    int finish_replication();

    std::string get_last_replicated_entry() const
    {
      return last_replicated_entry;
    }

    bool is_replication_complete() const
    {
      return is_complete;
    }

  private:
    int my_errno = 0;
    std::string const source_conf_path;
    std::string const dest_conf_path;
    std::string const source_keyring_path;
    std::string const dest_keyring_path;
    std::string const source_dir;            // dir for which snapshots are being replicated
    std::string const source_fs_name;
    std::string const dest_fs_name;
    std::string const old_snap_name;
    std::string const new_snap_name;
    std::string const source_auth_id;
    std::string const dest_auth_id;
    std::string source_snap_dir_name;  // ".snap" or something user configured
    std::string dest_snap_dir_name;    // ".snap" or something user configured

    snap_infos_t snap_info;


    // implementation specific
    bool is_complete = false;
    bool is_source_mounted = false;
    bool is_dest_mounted = false;
    struct ceph_mount_info *source_mnt = nullptr;
    struct ceph_mount_info *dest_mnt = nullptr;
    char *readbuf = new char[RollingChecksum::BLK_SIZE * 2];
    std::string last_replicated_entry;

    // private methods
    std::string get_snapshot_root_dir(snap_cluster_type_t sct, snap_type_t type) const;
    int generate_checksums(struct ceph_mount_info *mnt, int read_fd, int64_t len, ChecksumDataStore& cds);
    bool is_system_dir(char const *dir) const;
    int copy_blocks(std::string const& en, file_type_t ftype);
    struct timespec get_rctime(std::string const& snap_name);
    bool dir_exists_in_new_snap(dir_entries_map_t const& d_entries, std::string const& dir) const;
    bool dentry_exists_in_new_snap(dir_entries_map_t const& d_entries, std::string const& dir, std::string const& file) const;
    int del_tree(std::string const& entry_path) const;
    int update_utimes(std::string const& dest_entry, entry_info_t const *ei) const;
    int create_directory(struct ceph_mount_info *mnt, std::string const& dir);
};
#endif
