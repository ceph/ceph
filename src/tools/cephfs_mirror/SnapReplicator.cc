#include <queue>
#include <stack>

#include <dirent.h>
#include <string.h>

#include "SnapReplicator.h"

using namespace std::literals::string_literals;

class MD4Hash {
  private:
    static constexpr char const *hexchar = "0123456789abcdef";
    char buf[EVP_MAX_MD_SIZE * 2];
    EVP_MD_CTX *mdctx = nullptr;
    const EVP_MD *md = nullptr;
    char md_value[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;

  public:
    MD4Hash() : md(EVP_get_digestbyname("md4"))
    {
      ceph_assert(md);
      mdctx = EVP_MD_CTX_new();
      EVP_DigestInit_ex(mdctx, md, NULL);
    }

    ~MD4Hash()
    {
      if (mdctx)
        EVP_MD_CTX_free(mdctx);
    }

    void update(char const *buf, uint32_t len)
    {
      EVP_DigestUpdate(mdctx, buf, len);
    }

    std::string digest()
    {
      EVP_DigestFinal_ex(mdctx, reinterpret_cast<unsigned char *>(md_value), &md_len);
      return hex_digest();
    }

  private:
    std::string hex_digest()
    {
      int i = 0;

      for (unsigned int j = 0; j < md_len; j++) {
        auto ch = md_value[j];
        buf[i++] = hexchar[(ch>>4) & 0x0f]; // high nibble
        buf[i++] = hexchar[ch & 0x0f];      // low nibble
      }

      return std::string(buf, md_len*2);
    }
};

std::pair<std::string, std::string> get_dir_and_entry(std::string const& entry_path)
{
  std::string::size_type last_slash = entry_path.find_last_of('/');
  std::string dir = entry_path.substr(0, last_slash);
  last_slash++;
  std::string entry = entry_path.substr(last_slash, entry_path.size() - last_slash);

  return {dir, entry};
}


bool operator ==(struct timespec const& a, struct timespec const& b)
{
  return ((a.tv_sec == b.tv_sec) && (a.tv_nsec == b.tv_nsec));
}

int SnapReplicator::create_directory(struct ceph_mount_info *mnt, std::string const& dir)
{
  int rv = ceph_mkdirs(mnt, dir.data(), 0755);
  if (rv < 0 && -rv != EEXIST) {
    my_errno = -rv;
    return -1;
  }
  return 0;
}

int SnapReplicator::connect_to_cluster(snap_cluster_type_t sct)
{
  ceph_assert(sct == snap_cluster_type_t::SOURCE || sct == snap_cluster_type_t::DEST);

  struct ceph_mount_info *& mnt = (sct == SOURCE ? source_mnt : dest_mnt);
  std::string const& conf_path  = (sct == SOURCE ? source_conf_path : dest_conf_path);
  std::string const& fs_name    = (sct == SOURCE ? source_fs_name : dest_fs_name);
  std::string& snap_dir_name    = (sct == SOURCE ? source_snap_dir_name : dest_snap_dir_name);
  bool& is_mounted              = (sct == SOURCE ? is_source_mounted : is_dest_mounted);
  std::string const& auth_id    = (sct == SOURCE ? source_auth_id : dest_auth_id);

  int rv = ceph_create(&mnt, auth_id.data());
  if (rv < 0) {
    clog << "ceph_create failed!\n";
    my_errno = -rv;
    return -1;
  }
  clog << "created mount\n";

  rv = ceph_conf_read_file(mnt, conf_path.data());
  if (rv < 0) {
    clog << "ceph_conf_read_file cluster conf failed!\n";
    my_errno = -rv;
    return -1;
  }
  clog << "conf file read completed\n";

  rv = ceph_init(mnt);
  if (rv < 0) {
    clog << "ceph_init failed!\n";
    my_errno = -rv;
    return -1;
  }
  clog << "init completed\n";

  char buf[128];
  rv = ceph_conf_get(mnt, "client_snapdir", buf, sizeof(buf));
  if (rv < 0) {
    clog << "ceph_conf_get client_snapdir failed!\n";
    my_errno = -rv;
    return -1;
  }
  snap_dir_name = buf;
  clog << "client_snapdir is '" << snap_dir_name << "'\n";

  rv = ceph_select_filesystem(mnt, fs_name.data());
  if (rv < 0) {
    clog << "ceph_select_filesystem failed!\n";
    my_errno = -rv;
    return -1;
  }
  clog << "filesystem '" << fs_name << "' selected\n";

  if (sct == snap_cluster_type_t::DEST) {
    // we mount the root; then create the snap root dir; and unmount the root dir
    rv = ceph_mount(mnt, "/");
    if (rv < 0) {
      clog << "ceph_mount root failed!\n";
      my_errno = -rv;
      return -1;
    }
    clog << "filesystem '/' dir mounted\n";

    ceph_assert(ceph_is_mounted(mnt));

    rv = create_directory(mnt, source_dir);
    if (rv < 0) {
      clog << "error creating snapshot destination dir '" << source_dir << "'\n";
      return -1;
    }
    clog << "'" << source_dir << "' created\n";

    rv = ceph_unmount(mnt);
    if (rv < 0) {
      clog << "error unmounting '/' dir!\n";
      my_errno = -rv;
      return -1;
    }
    clog << "filesystem '/' dir unmounted\n";
  }

  /* INFO
   * can we directly mount a snapshot ? no, we can't
   * neither via fuse nor via kclient
   * so, we mount the source directory for which snapshot is being replicated
   */
  rv = ceph_mount(mnt, source_dir.data());
  if (rv < 0) {
    clog << "ceph_mount snap home dir failed!\n";
    my_errno = -rv;
    return -1;
  }
  clog << "filesystem '" << source_dir << "' dir mounted\n";

  is_mounted = true;

  return 0;
}

bool SnapReplicator::is_system_dir(char const *dir) const
{
  if ((dir[0] == '.' && dir[1] == '\0') ||
      (dir[0] == '.' && dir[1] == '.' && dir[2] == '\0'))
      return true;

  return false;
}

bool operator >(struct timespec lhs, struct timespec rhs)
{
  if (lhs.tv_sec > rhs.tv_sec)
    return true;

  if (lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec > rhs.tv_nsec)
    return true;

  return false;
}

struct dir_queue_item_t {
  std::string name;
  struct timespec ctime;

  dir_queue_item_t(std::string const& dir_name, struct timespec rctime) :
    name(dir_name), ctime(rctime)
  { }
};

int SnapReplicator::collect_snap_info(snap_type_t snap_type, struct timespec old_snapshot_ctime)
{
  if (snap_type == OLD_SNAP && old_snap_name == "")
    return 0; // nothing to collect here

  std::string snap_name = (snap_type == OLD_SNAP ? old_snap_name : new_snap_name);
  struct ceph_mount_info *mnt = source_mnt;
  std::string snap_root_dir = "/"s + source_snap_dir_name + "/"s + snap_name;
  std::queue<dir_queue_item_t> dir_queue;
  std::stack<struct timespec> dir_ctime_stack;

  struct timespec new_snapshot_ctime{ old_snapshot_ctime.tv_sec+1, 0 };

  dir_queue.push(dir_queue_item_t(""s, new_snapshot_ctime));

  while (!dir_queue.empty()) {
    ceph_dir_result *dirp = NULL;
    dir_queue_item_t dir_item = dir_queue.front();
    struct timespec dir_ctime = dir_item.ctime;
    std::string dir_name{dir_item.name};

    dir_queue.pop();

    std::string full_path = snap_root_dir + dir_name;

    if (ceph_opendir(mnt, full_path.data(), &dirp) != 0) {
      std::clog << __func__ << ":" << __LINE__ << ": error opening " << (snap_type == OLD_SNAP ? "old" : "new") << " snap dir " << dir_name << "\n";
      continue;
    }

    dir_ctime_stack.push(dir_ctime);

    struct dirent de;

    while (ceph_readdir_r(mnt, dirp, &de)) {
      if (is_system_dir(de.d_name))
        continue;

      struct stat st = {0, };
      std::string linked_file = ""s;
      std::string entry_name_path = full_path + "/"s + de.d_name;

      if (ceph_lstat(mnt, entry_name_path.data(), &st) != 0) {
        std::clog << __func__ << ":" << __LINE__ << ": error reading stat info for " << entry_name_path << " from " << (snap_type == OLD_SNAP ? "old" : "new") << " snap\n";
      } else {
        /* collect info about the dentry only if its ctime is greater than the
         * old snapshot's ctime
         * the old snapshot's ctime is helpful in pruning the search and 
         * restricting work only for items with ctime greater than old snapshot
         * ctime
         * if a directory's ctime is greater than old_snapshot_ctime then 
         * the whole sub-dir tree entries are captured for processing
         */
        if (dir_ctime > old_snapshot_ctime) {
          bool error = false;

          if (S_ISLNK(st.st_mode)) {
            char buf[PATH_MAX];
            int len = 0;
            if ((len = ceph_readlink(mnt, entry_name_path.data(), buf, sizeof(buf))) < 0) {
              std::clog << __func__ << ":" << __LINE__ << ": error reading symlink for entry " << entry_name_path << " in " << (snap_type == OLD_SNAP ? "old" : "new") << " snap\n";
              error = true;
            } else {
              linked_file = std::move(std::string(buf, len));
            }
          } else if (S_ISDIR(st.st_mode)) {
            std::string new_dir_path = dir_name + "/"s + de.d_name;
            if (st.st_ctim > old_snapshot_ctime) {
              dir_queue.push(dir_queue_item_t(new_dir_path, st.st_ctim));
            }
          }

          /* IMPORTANT
           * all regular files, whose parent-dir ctime is greater than
           * old_snapshot_ctime, are collected to enable checking for deleted
           * entries
           */
          if (!error) {
            // add entry path starting at snap root
            std::string entry_path = dir_name + "/"s + de.d_name;
            snap_info.add_entry(snap_type, de.d_ino, entry_path, st, linked_file);
          }
        }
      }
    }
    if (dirp) {
      ceph_closedir(mnt, dirp);
      dir_ctime = dir_ctime_stack.top();
      dir_ctime_stack.pop();
    }
  }
  return 0;
}

struct timespec SnapReplicator::get_rctime(std::string const& snap_name)
{
  struct timespec ts{0, 0};

  if (snap_name == ""s)
    return ts;

  std::string snap_dir_path = "/"s + source_snap_dir_name + "/"s + snap_name;

  struct stat st = {0,};

  ceph_assert(source_mnt);
  if (ceph_stat(source_mnt, snap_dir_path.data(), &st) < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": stat fetch failed for: " << snap_dir_path << "\n";
    return ts;
  }

  return st.st_ctim;
}

struct UpdateUtimesInfo {
  std::string dest_entry;
  entry_info_t const *ei;

  UpdateUtimesInfo(std::string const& _dest_entry, entry_info_t const*_ei) :
    dest_entry(_dest_entry), ei(_ei)
  { }
};

struct UpdateUtimesInfo_path_comparator_t {
  // shallow paths compare as lesser than deeper paths
  bool operator ()(UpdateUtimesInfo const& lhs, UpdateUtimesInfo const& rhs) const
  {
    std::string::size_type lhs_count = count_char(lhs.dest_entry, '/');
    std::string::size_type rhs_count = count_char(rhs.dest_entry, '/');

    if (lhs_count < rhs_count)
      return true;

    if (lhs_count == rhs_count)
      return (lhs.dest_entry.compare(rhs.dest_entry) < 0);

    return false;
  }
};

int SnapReplicator::update_utimes(std::string const& dest_entry, entry_info_t const *ei) const
{
  struct timeval times[2];
  struct timespec atim = ei->get_atime();
  struct timespec mtim = ei->get_mtime();

  // macros from <sys/time.h>
  TIMESPEC_TO_TIMEVAL(&times[0], &atim);
  TIMESPEC_TO_TIMEVAL(&times[1], &mtim);

  return ceph_lutimes(dest_mnt, dest_entry.data(), times);
}

std::string::size_type count_char(std::string const& str, char c)
{
  std::string::size_type n = 0;

  for (auto ch: str)
    n += (ch == c);

  return n;
}

bool SnapReplicator::dir_exists_in_new_snap(dir_entries_map_t const& d_entries, std::string const& dir) const
{
  return (d_entries.find(dir) != d_entries.end());
}

bool SnapReplicator::dentry_exists_in_new_snap(dir_entries_map_t const& d_entries, std::string const& dir, std::string const& file) const
{
  auto const& it = d_entries.find(dir);
  if (it != d_entries.end())
    return (it->second.find(file) != it->second.end());
  return false;
}

// delete the tree at the destination
int SnapReplicator::del_tree(std::string const& entry_path) const
{
  dir_entries_map_t const& od_entries = snap_info.get_dentries_map(snap_type_t::OLD_SNAP);
  ino_entry_map_t const& old_iem = snap_info.get_ino_entry_map(snap_type_t::OLD_SNAP);

  for (auto it = od_entries.at(entry_path).begin(); it != od_entries.at(entry_path).end(); ++it) {
    std::string entry = entry_path + "/"s + (*it);
    ino_t ino = snap_info.get_ino(entry, snap_type_t::OLD_SNAP);
    ceph_assert(ino != 0);
    entry_info_t const *oei = old_iem.at(ino);

    if (oei->is_dir()) {
      if (del_tree(entry) < 0)
        return -1;
      if (ceph_rmdir(dest_mnt, entry.data()) < 0)
        return -1;
    } else {
      if (ceph_unlink(dest_mnt, entry.data()) < 0)
        return -1;
    }
  }
  return 0;
}

// this is actually the "delete" phase
int SnapReplicator::replicate_phase1()
{
  int rv = 0;
  dir_entries_map_t const& od_entries = snap_info.get_dentries_map(snap_type_t::OLD_SNAP);
  dir_entries_map_t const& nd_entries = snap_info.get_dentries_map(snap_type_t::NEW_SNAP);
  ino_entry_map_t const& old_iem = snap_info.get_ino_entry_map(snap_type_t::OLD_SNAP);
  ino_entry_map_t const& new_iem = snap_info.get_ino_entry_map(snap_type_t::NEW_SNAP);

  for (auto old_de_it = od_entries.crbegin(); old_de_it != od_entries.crend(); ++old_de_it) {
    auto const& files = old_de_it->second;
    auto const& dir = old_de_it->first;

    /* if this diretory does not exist in the list of dentries of the new snap
     * then it does not contain any updates; so we ignore it;
     * any deletes/renames from a directory will cause the ctime of parent
     * dir to be updated causing it to be included in the list of changed files
     */
    if (!dir_exists_in_new_snap(nd_entries, dir)) {
      std::clog << __func__ << ":" << __LINE__ << ": skipping dir: " << dir << "\n";
      continue;
    }

    for (auto const& file: files) {
      std::string const& dentry_path = dir + "/"s + file;
      std::clog << __func__ << ":" << __LINE__ << ": testing presence of " << dentry_path << "\n";
      if (!dentry_exists_in_new_snap(nd_entries, dir, file)) {
        ino_t old_ino = snap_info.get_ino(dentry_path, snap_type_t::OLD_SNAP);
        ceph_assert(old_ino != 0);
        entry_info_t const *oei = old_iem.at(old_ino);

        if (oei->is_dir()) {
          entry_info_t const *nei = new_iem.at(old_ino);

          if (nei && nei->is_dir()) {
            // check if such an entry exists in new_snap_entries
            entry_ino_map_t const& nei_map = snap_info.get_entries(snap_type_t::NEW_SNAP);
            std::string const& new_entry_path = *(nei->get_entry_names().begin());

            if (nei_map.find(new_entry_path) != nei_map.end()) {
              // this is indeed a simple rename
              std::clog << __func__ << ":" << __LINE__ << ": renaming dir:" << dentry_path << " to " << new_entry_path << "\n";
              ceph_rename(dest_mnt, dentry_path.data(), new_entry_path.data());
            } else {
              goto rename_dir_the_hard_way;
            }
          } else {
rename_dir_the_hard_way:
            std::clog << __func__ << ":" << __LINE__ << ": deleting dir:" << dentry_path << "\n";

            if ((rv = del_tree(dentry_path)) < 0) {
              my_errno = -rv;
              /* this shouldn't happen when path_comparator is used and we are
               * iterating over the diretories in depth first order
               * the path_comprator takes care of deeper directories as greater
               * than shallower directories
               */
              std::clog << __func__ << ":" << __LINE__ << ": error removing dir tree " << dentry_path << " (" << my_errno
                << ":" << strerror(my_errno) << "); maybe its not empty\n";
            } else {
              ceph_rmdir(dest_mnt, dentry_path.data());
            }
          }
        } else {
          entry_info_t const *nei = new_iem.at(old_ino);

          if (nei && nei->get_type() == oei->get_type()) {
            // check if such an entry exists in new_snap_entries
            entry_ino_map_t const& nei_map = snap_info.get_entries(snap_type_t::NEW_SNAP);
            if (nei->get_entry_names().size() > 1)
              goto rename_file_the_hard_way;

            std::string const& new_entry_path = *(nei->get_entry_names().begin());

            if (nei_map.find(new_entry_path) != nei_map.end()) {
              // this is indeed a simple rename
              std::clog << __func__ << ":" << __LINE__ << ": renaming dir:" << dentry_path << " to " << new_entry_path << "\n";
              ceph_rename(dest_mnt, dentry_path.data(), new_entry_path.data());
            } else {
              goto rename_file_the_hard_way;
            }
          } else {
rename_file_the_hard_way:
            std::clog << __func__ << ":" << __LINE__ << ": deleting file:" << dentry_path << "\n";

            if ((rv = ceph_unlink(dest_mnt, dentry_path.data())) < 0) {
              my_errno = -rv;
              // this shouldn't happen when path_comparator is used
              std::clog << __func__ << ":" << __LINE__ << ": error removing file " << dentry_path << " (" << my_errno
                << ":" << strerror(my_errno) << "); maybe its not empty\n";
            }
          }
        }
      }
    }
  }
  return 0;
}

/* restart_after_entry: if "", implies start at the beginning;
 *                      don't skip anything
 *                    : if non-empty, implies skip until that entry and redo
 *                      things for entries laster than that entry
 */
int SnapReplicator::replicate_phase2(std::string const& restart_after_entry)
{
  // INFO snapshots are not mountable
  bool proceed = false;
  std::string dest_root = source_dir + "/"s + dest_snap_dir_name;
  ino_entry_map_t const& old_iem = snap_info.get_ino_entry_map(snap_type_t::OLD_SNAP);
  ino_entry_map_t const& new_iem = snap_info.get_ino_entry_map(snap_type_t::NEW_SNAP);
  entry_ino_map_t const& nsentries = snap_info.get_entries(snap_type_t::NEW_SNAP);
  std::vector<std::string> entry_list;
  std::vector<struct UpdateUtimesInfo> dirs_to_update_utimes;
  bool is_copy_fault = false;

  is_complete = false;

  if (restart_after_entry == "//DONE")
    goto copy_done;

  for (auto& it: nsentries)
    entry_list.push_back(it.first);

  // each entry has a path prefix as well which starts from the snapshot root
  std::sort(entry_list.begin(), entry_list.end());

  for (auto& en: entry_list) {
    if (!proceed) {
      if (restart_after_entry == ""s)
        proceed = true;
      else {
        if (restart_after_entry != en)
          continue;
        else {
          proceed = true;
          continue;
        }
      }
    }

    std::clog << __func__ << ":" << __LINE__ << ": processing:" << en << "\n";

    ino_t old_ino = snap_info.get_ino(en, snap_type_t::OLD_SNAP);
    ino_t new_ino = snap_info.get_ino(en, snap_type_t::NEW_SNAP);
    presence_t pres = snap_info.get_entry_presence(en);

    ceph_assert(pres > presence_t::PRESENT_IN_NONE);

    entry_info_t const *oei = nullptr;
    entry_info_t const *nei = nullptr;

    if (old_ino > 0)
      oei = old_iem.at(old_ino);

    if (new_ino > 0)
      nei = new_iem.at(new_ino);

    //std::string dest_entry = source_dir + en; // en has a '/' prefix already

    if (pres == presence_t::PRESENT_IN_BOTH) {
      if (!(*oei == *nei)) {
        // file has differences
        /* TODO what if the file was deleted and a new file with the same name
         * but a different type was created but got assigned the same inode number
         */
        ceph_assert(oei->get_type() == nei->get_type());

        if (nei->is_reg()) {
          if (copy_blocks(en, file_type_t::OLD_FILE) != 0) {
            std::clog << __func__ << ":" << __LINE__ << ": error copying updated blocks for " << en << "\n";
            continue;
          }
          if (!nei->has_same_size(oei->get_size())) {
            ceph_truncate(dest_mnt, en.data(), nei->get_size());
          }
          if (!nei->has_same_mtime(oei->get_mtime())) {
            update_utimes(en.data(), nei);
          }
        } else if (nei->is_lnk()) {
          /* TODO check if the symlink points to the same path;
           * if not, then recreate the symlink
           */
        } else if (nei->is_dir()) {
          dirs_to_update_utimes.push_back(UpdateUtimesInfo(en, nei));
        }

        if (!nei->has_same_uid(oei->get_uid()) || !nei->has_same_gid(oei->get_gid()))
          ceph_lchown(dest_mnt, en.data(), nei->get_uid(), nei->get_gid());
      }
    } else if (pres == presence_t::PRESENT_IN_OLD) {
      /* with phase1 and phase2 processing, this situation doesn't ever arise
       * in phase2 processing since we'll always be looking at dentries in
       * the new snapshot
       */
      ceph_assert(false);
#if 0
      if (!nei) {
        // inode not reused; looks like file/dir is indeed deleted
        if (oei->is_dir()) {
          // recursive unlink
        } else {
          /* if this turns out to be a recursive rmdir, then the files get
           * deleted first leaving empty dirs behind; the dirs are collected
           * in dirs_to_delete and removed at the end of the processing
           */
        }
      } else {
        /* inode reused; but with different name
         * probably a rename
         */
      }
#endif
    } else if (pres == presence_t::PRESENT_IN_NEW) {
      if (oei) {
      /* TODO if inode is found in old entry list, then the inode has been
       * reassigned to a different file
       * most probably a rename is file types are same
       */
      } else {
        if (nei->is_dir()) {
          int rv = 0;
          if ((rv = ceph_mkdir(dest_mnt, en.data(), nei->get_perms())) < 0) {
            if (rv == -EEXIST) {
              std::clog << __func__ << ":" << __LINE__ << ": dir " << en << " already exists; maybe its been renamed earlier\n";
              rv = 0;
            } else {
              std::clog << __func__ << ":" << __LINE__ << ": error creating dir " << en << "\n";
            }
          }
          if (rv == 0)
            dirs_to_update_utimes.push_back(UpdateUtimesInfo(en, nei));
        } else if (nei->is_reg()) {
          /* Test to see if there's an old entry;
           * The entry may have been renamed; so test the mtimes and only copy
           * blocks if mtime is different
           */
          ino_entry_map_t const& old_iem = snap_info.get_ino_entry_map(snap_type_t::OLD_SNAP);
          entry_info_t const *oei = old_iem.at(nei->get_ino());
          auto [ndir, nentry] = get_dir_and_entry(en);
          std::string old_entry = (oei ? *(oei->get_entry_names().begin()) : "");
          auto [odir, oentry] = (old_entry != "" ? get_dir_and_entry(old_entry) : std::make_pair("",""));

          /* hard links cause file to be erased and recreated
           * for now, we just attempt to detect a complex rename with
           * ndir != odir
           */
          if (!oei || (ndir != odir) || !nei->has_same_mtime(oei->get_mtime())) {
            if (copy_blocks(en, file_type_t::NEW_FILE) != 0) {
              std::clog << __func__ << ":" << __LINE__ << ": error copying blocks for " << en << "\n";
              is_copy_fault = true;
              break;
            } else {
              ceph_truncate(dest_mnt, en.data(), nei->get_size());
            }
          }
        } else if (nei->is_lnk()) {
          int rv = 0;
          if ((rv = ceph_symlink(dest_mnt, nei->get_linked_file().data(), en.data())) < 0) {
            if (rv == -EEXIST)
              std::clog << __func__ << ":" << __LINE__ << ": symlink " << en << " for " << nei->get_linked_file() << " already exists; may have been renamed\n";
            else
              std::clog << __func__ << ":" << __LINE__ << ": error creating symlink " << en << " for " << nei->get_linked_file() << ";\n";
          }
        }
      }
    }
    last_replicated_entry = en;
  }
  if (is_copy_fault)
    return -1;

  if (dirs_to_update_utimes.size()) {
    std::sort(dirs_to_update_utimes.begin(), dirs_to_update_utimes.end(), UpdateUtimesInfo_path_comparator_t());
    std::reverse(dirs_to_update_utimes.begin(), dirs_to_update_utimes.end());
    for (auto& uudir: dirs_to_update_utimes) {
      if (update_utimes(uudir.dest_entry, uudir.ei) < 0) {
        std::clog << __func__ << ":" << __LINE__ << ": utimes update failed for " << uudir.dest_entry << "\n";
      }
    }
  }

  last_replicated_entry = "//DONE";

copy_done:
  // create the snap dir on the destination
  std::string const& new_snap_dir = dest_snap_dir_name + "/"s + new_snap_name;

  int rv = 0;
  if ((rv = ceph_mkdir(dest_mnt, new_snap_dir.data(), 0755)) < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": failed to create snapshot dir (" << new_snap_dir << ") on destination cluster\n";
    my_errno = -rv;
    return -1;
  }

  is_complete = true;

  return 0;
}

std::string SnapReplicator::get_snapshot_root_dir(snap_cluster_type_t sct, snap_type_t type) const
{
  if (sct == snap_cluster_type_t::SOURCE) {
    ceph_assert(type != snap_type_t::NO_SNAP);
    if (type == snap_type_t::OLD_SNAP)
      return ("/"s + source_snap_dir_name + "/" + old_snap_name);
    else
      return ("/"s + source_snap_dir_name + "/" + new_snap_name);
  } else {
    return source_dir + "/" + dest_snap_dir_name + "/" + new_snap_name;
  }
}

/* to be used only for generating block level checksums for files 
 * in old snapshot
 */
int SnapReplicator::generate_checksums(
  struct ceph_mount_info *mnt,
  int read_fd,
  int64_t len,
  ChecksumDataStore& cds)
{
  char buf[RollingChecksum::BLK_SIZE];
  int rv = 0;
  int64_t read = 0;

  while (read != len) {
    rv = ceph_read(mnt, read_fd, buf, sizeof(buf), read);
    if (rv < 0) {
      std::clog << __func__ << ":" << __LINE__ << ": ceph_read failed\n";
      my_errno = -rv;
      return -1;
    }

    RollingChecksum rc(buf, rv);
    /* each block gets its own MD4 hash;
     * its not file-wide running update
     */
    MD4Hash hash;
    hash.update(buf, rv);

    cds.add_checksum(read, rc.get(), hash.digest());

    read += rv;
  }
  return 0;
}

// FIXME renames are not yet handled; also not handled renames with updates
int SnapReplicator::copy_blocks(std::string const& en, file_type_t ftype)
{
  ino_entry_map_t const& old_iem = snap_info.get_ino_entry_map(snap_type_t::OLD_SNAP);
  ino_entry_map_t const& new_iem = snap_info.get_ino_entry_map(snap_type_t::NEW_SNAP);
  ino_t old_ino = snap_info.get_ino(en, snap_type_t::OLD_SNAP);
  ino_t new_ino = snap_info.get_ino(en, snap_type_t::NEW_SNAP);
  bool is_error = false;

  if (ftype == file_type_t::OLD_FILE) {
    off_t old_size = old_iem.at(old_ino)->get_size();

    if (old_size == 0) {
      std::clog << __func__ << ":" << __LINE__ << ": old file size is zero; copying all new file blocks\n";
      goto copy_all_blocks;
    }

    std::string old_root = get_snapshot_root_dir(snap_cluster_type_t::SOURCE, snap_type_t::OLD_SNAP);

    int rv = ceph_open(source_mnt, (old_root + en).data(), O_RDONLY, 0);

    if (rv < 0) {
      std::clog << __func__ << ":" << __LINE__ << ": ceph_open(" << (old_root + en) << ") failed\n";
      my_errno = -rv;
      return -1;
    }

    int read_fd = rv;

    ChecksumDataStore old_snap_cds;

    rv = generate_checksums(source_mnt, read_fd, old_size, old_snap_cds);
    ceph_close(source_mnt, read_fd);

    if (rv != 0) {
      std::clog << __func__ << ":" << __LINE__ << ": generate_checksums() failed\n";
      return -1;
    }

    std::string new_root = get_snapshot_root_dir(snap_cluster_type_t::SOURCE, snap_type_t::NEW_SNAP);
    rv = ceph_open(source_mnt, (new_root + en).data(), O_RDONLY, 0);
    if (rv < 0) {
      std::clog << __func__ << ":" << __LINE__ << ": ceph_open(" << (new_root + en) << ") failed\n";
      my_errno = -rv;
      return -1;
    }
    read_fd = rv;

    off_t read = 0;
    off_t const new_size = new_iem.at(new_ino)->get_size();

    if (new_size == 0) {
      std::clog << __func__ << ":" << __LINE__ << ": new file size is zero; nothing to copy\n";
      return 0;
    }

    std::clog << __func__ << ":" << __LINE__ << ": starting copy of file blocks\n";
    // if length of the file is greater than 1 RollingChecksum::BLK_SIZE

    std::clog << __func__ << ":" << __LINE__ << ": new file size, " << new_size << " > 1 block (" << RollingChecksum::BLK_SIZE << ")\n";
    // copy more than one block
    // off_t const new_len = new_iem.at(new_ino)->get_size();
    if (new_size == 0) {
      std::clog << __func__ << ":" << __LINE__ << ": new file size is zero\n";
      return 0;
    }

    rv = ceph_open(dest_mnt, (en).data(), O_CREAT|O_WRONLY, new_iem.at(new_ino)->get_perms());
    if (rv < 0) {
      my_errno = -rv;
      return -1;
    }

    int write_fd = rv;
    off_t const min_size = std::min(old_size, new_size);

    read = 0;
    while (read < min_size) {
      rv = ceph_read(source_mnt, read_fd, readbuf, RollingChecksum::BLK_SIZE, read);

      if (rv < 0) {
        std::clog << __func__ << ":" << __LINE__ << ": failed while reading from old file\n";
        my_errno = -rv;
        is_error = true;
        break;
      }

      RollingChecksum rc(readbuf, rv);

      if (!old_snap_cds.does_match_weak(read, rc.get()) ||
          ({MD4Hash hash; hash.update(readbuf, rv);
           !old_snap_cds.does_match_strong(read, rc.get(), hash.digest());})) {
        std::clog << __func__ << ":" << __LINE__ << ": copy " << rv << " bytes @" << read << "\n";
        rv = ceph_write(dest_mnt, write_fd, readbuf, rv, read);
        if (rv < 0) {
          std::clog << __func__ << ":" << __LINE__ << ": failed while writing to new file\n";
          my_errno = -rv;
          is_error = true;
          break;
        }
      }

      read += rv;
    }

    if (!is_error && min_size < new_size) {
      // copy over the remaining suffix chunk to the destination
      while (read < new_size) {
        if ((rv = ceph_read(source_mnt, read_fd, readbuf, RollingChecksum::BLK_SIZE, read)) < 0) {
          std::clog << __func__ << ":" << __LINE__ << ": source file read with ceph_read(" << read_fd << ", " << RollingChecksum::BLK_SIZE << ", " << read << ") failed\n";
          my_errno = -rv;
          is_error = true;
          break;
        }
        int read_bytes = rv;
        if ((rv = ceph_write(dest_mnt, write_fd, readbuf, rv, read)) < 0) {
          std::clog << __func__ << ":" << __LINE__ << ": destination file write with ceph_write(" << write_fd << ", " << read_bytes << ", " << read << ") failed\n";
          my_errno = -rv;
          is_error = true;
          break;
        }
        read += rv;
      }
    }

    if (write_fd)
      ceph_close(dest_mnt, write_fd);
    if (read_fd)
      ceph_close(source_mnt, read_fd);
  } else if (ftype == file_type_t::NEW_FILE) {
copy_all_blocks:
    if (new_iem.at(new_ino)->get_size() == 0) {
      std::clog << __func__ << ":" << __LINE__ << ": new file size is zero; nothing to copy\n";
      return 0;
    }

    std::string new_root = get_snapshot_root_dir(snap_cluster_type_t::SOURCE, snap_type_t::NEW_SNAP);
    int rv = ceph_open(source_mnt, (new_root + en).data(), O_RDONLY, 0);
    if (rv < 0) {
      std::clog << __func__ << ":" << __LINE__ << ": opening new source file with ceph_open(" << (new_root + en) << ", O_RDONLY) failed\n";
      my_errno = -rv;
      return -1;
    }
    int read_fd = rv;
    
    rv = ceph_open(dest_mnt, en.data(), O_CREAT|O_WRONLY, new_iem.at(new_ino)->get_perms());
    if (rv < 0) {
      std::clog << __func__ << ":" << __LINE__ << ": opening new destination file with ceph_open(" << (en) << ", O_CREAT) failed\n";
      my_errno = -rv;
      ceph_close(source_mnt, read_fd);
      return -1;
    }
    int write_fd = rv;
    
    off_t read = 0;
    off_t const len = new_iem.at(new_ino)->get_size();

    while (read < len) {
      if ((rv = ceph_read(source_mnt, read_fd, readbuf, RollingChecksum::BLK_SIZE, read)) < 0) {
        std::clog << __func__ << ":" << __LINE__ << ": source file read with ceph_read(" << read_fd << ", " << RollingChecksum::BLK_SIZE << ", " << read << ") failed\n";
        my_errno = -rv;
        is_error = true;
        break;
      }
      if ((rv = ceph_write(dest_mnt, write_fd, readbuf, rv, read)) < 0) {
        std::clog << __func__ << ":" << __LINE__ << ": destination file write with ceph_write(" << write_fd << ", " << rv << ", " << read << ") failed\n";
        my_errno = -rv;
        is_error = true;
        break;
      }
      read += rv;
    }
    ceph_close(dest_mnt, write_fd);
    ceph_close(source_mnt, read_fd);
  }
  if (is_error)
    return -1;

  return 0;
}

int SnapReplicator::prepare_to_replicate()
{
  std::clog << __func__ << ":" << __LINE__ << ": collecting info from old snapshot\n";
  if (collect_snap_info(snap_type_t::OLD_SNAP, {0,0}) != 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: while collecting info from old snapshot\n";
    return -1;
  }

  struct timespec old_rctime = get_rctime(old_snap_name);

  std::clog << __func__ << ":" << __LINE__ << ": old rc_time:" << old_rctime.tv_sec << "." << std::setw(9) << std::setfill('0') << old_rctime.tv_nsec << "\n";
  std::clog << __func__ << ":" << __LINE__ << ": collecting info from new snapshot\n";
  if (collect_snap_info(snap_type_t::NEW_SNAP, old_rctime) != 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: while collecting info from new snapshot\n";
    return -1;
  }

  return 0;
}

int SnapReplicator::finish_replication()
{
  int rv = 0;

  if (is_dest_mounted) {
    rv = ceph_sync_fs(dest_mnt);
    ceph_unmount(dest_mnt);
    ceph_release(dest_mnt);
    dest_mnt = nullptr;
    is_dest_mounted = false;
  }

  if (is_source_mounted) {
    ceph_unmount(source_mnt);
    ceph_release(source_mnt);
    source_mnt = nullptr;
    is_source_mounted = false;
  }

  return rv;
}
