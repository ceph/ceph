// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_PEER_REPLAYER_H
#define CEPHFS_MIRROR_PEER_REPLAYER_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "ServiceDaemon.h"
#include "Types.h"

#include <boost/optional.hpp>

namespace cephfs {
namespace mirror {

class FSMirror;
class PeerReplayerAdminSocketHook;


class PeerReplayer {
public:
  PeerReplayer(CephContext *cct, FSMirror *fs_mirror,
               RadosRef local_cluster, const Filesystem &filesystem,
               const Peer &peer, const std::set<std::string, std::less<>> &directories,
               MountRef mount, ServiceDaemon *service_daemon);
  ~PeerReplayer();

  // file descriptor "triplet" for synchronizing a snapshot
  // w/ an added MountRef for accessing "previous" snapshot.
  struct FHandles {
    // open file descriptor on the snap directory for snapshot
    // currently being synchronized. Always use this fd with
    // @m_local_mount.
    int c_fd;

    // open file descriptor on the "previous" snapshot or on
    // dir_root on remote filesystem (based on if the snapshot
    // can be used for incremental transfer). Always use this
    // fd with p_mnt which either points to @m_local_mount (
    // for local incremental comparison) or @m_remote_mount (
    // for remote incremental comparison).
    int p_fd;
    MountRef p_mnt;

    // open file descriptor on dir_root on remote filesystem.
    // Always use this fd with @m_remote_mount.
    int r_fd_dir_root;
  };

  // initialize replayer for a peer
  int init();

  // shutdown replayer for a peer
  void shutdown();

  // add a directory to mirror queue
  void add_directory(std::string_view dir_root);

  // remove a directory from queue
  void remove_directory(std::string_view dir_root);

  // admin socket helpers
  void peer_status(Formatter *f);

  // reopen logs
  void reopen_logs();

  using Snapshot = std::pair<std::string, uint64_t>;

  class C_MirrorContext : public Context {
  public:
    C_MirrorContext(std::atomic<int64_t> &op_counter, Context *fin,
                    PeerReplayer *replayer)
        : op_counter(op_counter), fin(fin), replayer(replayer),
          m_peer(replayer->m_peer) {}
    virtual void finish(int r) = 0;
    void complete(int r) override {
      finish(r);
      dec_counter();
      delete this;
    }
    void inc_counter() { ++op_counter; }

  protected:
    std::atomic<int64_t> &op_counter;
    Context *fin;
    PeerReplayer *replayer;
    Peer &m_peer; // just for using dout

  private:
    void dec_counter() {
      --op_counter;
      if (op_counter <= 0) {
        fin->complete(0);
      }
    }
  };

  class OpHandlerThreadPool {
  public:
    OpHandlerThreadPool() {}
    OpHandlerThreadPool(int _num_file_threads, int _num_other_threads);
    void activate();
    void deactivate();
    void do_file_task_async(C_MirrorContext *task);
    bool do_other_task_async(C_MirrorContext *task);
    void handle_other_task_async_sync(C_MirrorContext *task);
    bool is_other_task_queue_full_unlocked();
    std::atomic<int> fcount, ocount;

  private:
    void run_file_task();
    void run_other_task();
    void drain_queue();
    std::queue<C_MirrorContext *> file_task_queue;
    std::queue<C_MirrorContext *> other_task_queue;
    std::condition_variable pick_file_task;
    std::condition_variable give_file_task;
    std::condition_variable pick_other_task;
    std::mutex fmtx, omtx;
    static const int task_queue_limit = 100000;
    std::vector<std::thread> file_workers;
    std::vector<std::thread> other_workers;
    std::atomic<bool> stop_flag;
  };

  class C_DoDirSync : public C_MirrorContext {
  public:
    C_DoDirSync(const std::string &dir_root, const std::string &cur_path,
                const struct ceph_statx &cur_stx, ceph_dir_result *root_dirp,
                bool create_fresh, const FHandles &fh,
                std::atomic<bool> &canceled, std::atomic<bool> &failed,
                std::atomic<int64_t> &op_counter, Context *fin,
                PeerReplayer *replayer)
        : C_MirrorContext(op_counter, fin, replayer), dir_root(dir_root),
          cur_path(cur_path), cur_stx(cur_stx), root_dirp(root_dirp),
          create_fresh(create_fresh), fh(fh), canceled(canceled),
          failed(failed) {}
    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string cur_path;
    struct ceph_statx cur_stx;
    ceph_dir_result *root_dirp;
    bool create_fresh;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class C_DoDirSyncSnapDiff : public C_MirrorContext {
  public:
    C_DoDirSyncSnapDiff(const std::string &dir_root,
                        const std::string &cur_path,
                        const struct ceph_statx &cur_stx,
                        ceph_snapdiff_info *snapdiff_info, const FHandles &fh,
                        const Snapshot &current, const Snapshot &prev,
                        std::atomic<bool> &canceled, std::atomic<bool> &failed,
                        std::atomic<int64_t> &op_counter, Context *fin,
                        PeerReplayer *replayer)
        : C_MirrorContext(op_counter, fin, replayer), dir_root(dir_root),
          cur_path(cur_path), cur_stx(cur_stx), snapdiff_info(snapdiff_info),
          fh(fh), current(current), prev(prev), canceled(canceled),
          failed(failed) {}
    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string cur_path;
    struct ceph_statx cur_stx;
    ceph_snapdiff_info *snapdiff_info;
    const FHandles &fh;
    const Snapshot &current;
    const Snapshot &prev;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class C_TransferAndSyncFile : public C_MirrorContext {
  public:
    C_TransferAndSyncFile(const std::string &dir_root, const std::string &epath,
                          const struct ceph_statx &stx, const FHandles &fh,
                          uint64_t change_mask, std::atomic<bool> &canceled,
                          std::atomic<bool> &failed,
                          std::atomic<int64_t> &op_counter, Context *fin,
                          PeerReplayer *replayer)
        : C_MirrorContext(op_counter, fin, replayer), dir_root(dir_root),
          epath(epath), stx(stx), fh(fh), change_mask(change_mask),
          canceled(canceled), failed(failed) {}

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    struct ceph_statx stx;
    const FHandles &fh;
    bool change_mask;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };

  class C_CleanUpRemoteDir : public C_MirrorContext {
  public:
    C_CleanUpRemoteDir(const std::string &dir_root, const std::string &epath,
                       const FHandles &fh, std::atomic<bool> &canceled,
                       std::atomic<bool> &failed,
                       std::atomic<int64_t> &op_counter, Context *fin,
                       PeerReplayer *replayer)
        : C_MirrorContext(op_counter, fin, replayer), dir_root(dir_root),
          epath(epath), fh(fh), canceled(canceled), failed(failed) {}

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    const FHandles &fh;
    std::atomic<bool> &canceled;
    std::atomic<bool> &failed;
  };
  friend class C_MirrorContext;
  friend class C_DoDirSync;
  friend class C_TransferFile;
  friend class C_CleanUpRemoteDir;
  friend class C_DoDirSyncSnapDiff;

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  inline static const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
  inline static const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";

  bool is_stopping() {
    return m_stopping;
  }

  struct Replayer;
  class SnapshotReplayerThread : public Thread {
  public:
    SnapshotReplayerThread(PeerReplayer *peer_replayer)
      : m_peer_replayer(peer_replayer) {
    }

    void *entry() override {
      m_peer_replayer->run(this);
      return 0;
    }

  private:
    PeerReplayer *m_peer_replayer;
  };

  struct DirRegistry {
    int fd;
    std::atomic<bool> canceled;
    SnapshotReplayerThread *replayer;
    std::atomic<bool> failed;
    int failed_reason;
    DirRegistry(): canceled(false), failed(false) {}
    DirRegistry(const DirRegistry &registry)
        : fd(registry.fd), canceled(registry.canceled.load()),
          replayer(registry.replayer), failed(registry.failed.load()),
          failed_reason(registry.failed_reason) {}
    DirRegistry &operator=(const DirRegistry &registry) {
      if (this != &registry) {
        fd = registry.fd;
        canceled.store(registry.canceled.load());
        replayer = registry.replayer;
        failed.store(registry.failed.load());
        failed_reason = registry.failed_reason;
      }
      return *this;
    }
  };

  struct SyncEntry {
    std::string epath;
    ceph_dir_result *dirp; // valid for directories
    ceph_snapdiff_info* sd_info;
    ceph_snapdiff_entry_t sd_entry;
    struct ceph_statx stx;
    bool create_fresh = false;
    // set by incremental sync _after_ ensuring missing entries
    // in the currently synced snapshot have been propagated to
    // the remote filesystem.
    bool remote_synced = false;

    SyncEntry(std::string_view path, const struct ceph_statx &stx)
        : epath(path), stx(stx) {}
    SyncEntry(std::string_view path, ceph_dir_result *dirp,
              const struct ceph_statx &stx, bool create_fresh = false)
        : epath(path), dirp(dirp), stx(stx), create_fresh(create_fresh) {}
    SyncEntry(std::string_view path, ceph_snapdiff_info *sd_info,
              const ceph_snapdiff_entry_t &sd_entry,
              const struct ceph_statx &stx)
        : epath(path), sd_info(sd_info), sd_entry(sd_entry), stx(stx) {}

    bool is_directory() const {
      return S_ISDIR(stx.stx_mode);
    }

    bool needs_remote_sync() const {
      return remote_synced;
    }
    void set_remote_synced() {
      remote_synced = true;
    }
  };

  // stats sent to service daemon
  struct ServiceDaemonStats {
    uint64_t failed_dir_count = 0;
    uint64_t recovered_dir_count = 0;
  };

  struct SnapSyncStat {
    uint64_t nr_failures = 0; // number of consecutive failures
    boost::optional<monotime> last_failed; // lat failed timestamp
    boost::optional<std::string> last_failed_reason;
    bool failed = false; // hit upper cap for consecutive failures
    boost::optional<std::pair<uint64_t, std::string>> last_synced_snap;
    boost::optional<std::pair<uint64_t, std::string>> current_syncing_snap;
    uint64_t synced_snap_count = 0;
    uint64_t deleted_snap_count = 0;
    uint64_t renamed_snap_count = 0;
    monotime last_synced = clock::zero();
    boost::optional<double> last_sync_duration;
    boost::optional<uint64_t> last_sync_bytes; //last sync bytes for display in status
    uint64_t sync_bytes = 0; //sync bytes counter, independently for each directory sync.
  };

  void _inc_failed_count(const std::string &dir_root) {
    auto max_failures = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_consecutive_failures_per_directory");
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_failed = clock::now();
    if (++sync_stat.nr_failures >= max_failures && !sync_stat.failed) {
      sync_stat.failed = true;
      ++m_service_daemon_stats.failed_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_FAILED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.failed_dir_count);
    }
  }
  void _reset_failed_count(const std::string &dir_root) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    if (sync_stat.failed) {
      ++m_service_daemon_stats.recovered_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.recovered_dir_count);
    }
    sync_stat.nr_failures = 0;
    sync_stat.failed = false;
    sync_stat.last_failed = boost::none;
    sync_stat.last_failed_reason = boost::none;
  }

  void _set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_synced_snap = std::make_pair(snap_id, snap_name);
    sync_stat.current_syncing_snap = boost::none;
  }
  void set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.sync_bytes = 0;
  }
  void set_current_syncing_snap(const std::string &dir_root, uint64_t snap_id,
                                const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.current_syncing_snap = std::make_pair(snap_id, snap_name);
  }
  void clear_current_syncing_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.current_syncing_snap = boost::none;
  }
  void inc_deleted_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat.deleted_snap_count;
  }
  void inc_renamed_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat.renamed_snap_count;
  }
  void set_last_synced_stat(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name, double duration) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.last_synced = clock::now();
    sync_stat.last_sync_duration = duration;
    sync_stat.last_sync_bytes = sync_stat.sync_bytes;
    ++sync_stat.synced_snap_count;
  }
  void inc_sync_bytes(const std::string &dir_root, const uint64_t& b) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat.sync_bytes += b;
  }
  bool should_backoff(std::atomic<bool> &canceled, std::atomic<bool> &failed,
                      int *retval) {
    if (m_fs_mirror->is_blocklisted()) {
      *retval = -EBLOCKLISTED;
      return true;
    }

    if (m_stopping) {
      // ceph defines EBLOCKLISTED to ESHUTDOWN (108). so use
      // EINPROGRESS to identify shutdown.
      *retval = -EINPROGRESS;
      return true;
    }
    if (canceled || failed) {
      *retval = -ECANCELED;
      return true;
    }

    *retval = 0;
    return false;
  }

  int get_failed_reason(const std::string &dir_root) {
    std::scoped_lock lock(m_lock);
    auto &dr = m_registered.at(dir_root);
    return dr.failed_reason;
  }

  void mark_failed(const std::string &dir_root, int reason) {
    std::scoped_lock lock(m_lock);
    auto it = m_registered.find(dir_root);
    if (it == m_registered.end()) {
      return;
    }
    if (it->second.failed) {
      return;
    }
    it->second.failed = true;
    it->second.failed_reason = reason;
  }

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  RadosRef m_local_cluster;
  Filesystem m_filesystem;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry> m_registered;
  std::vector<std::string> m_directories;
  std::map<std::string, SnapSyncStat> m_snap_sync_stats;
  MountRef m_local_mount;
  ServiceDaemon *m_service_daemon;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
  OpHandlerThreadPool op_handler_context;
  ceph::condition_variable m_cond;
  RadosRef m_remote_cluster;
  MountRef m_remote_mount;
  std::atomic<bool> m_stopping = false;
  SnapshotReplayers m_replayers;

  ServiceDaemonStats m_service_daemon_stats;

  PerfCounters *m_perf_counters;

  void run(SnapshotReplayerThread *replayer);

  boost::optional<std::string> pick_directory();
  int register_directory(const std::string &dir_root, SnapshotReplayerThread *replayer);
  void unregister_directory(const std::string &dir_root);
  int try_lock_directory(const std::string &dir_root, SnapshotReplayerThread *replayer,
                         DirRegistry *registry);
  void unlock_directory(const std::string &dir_root, const DirRegistry &registry);
  int sync_snaps(const std::string &dir_root, std::unique_lock<ceph::mutex> &locker);


  int build_snap_map(const std::string &dir_root, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);

  int propagate_snap_deletes(const std::string &dir_root, const std::set<std::string> &snaps);
  int propagate_snap_renames(const std::string &dir_root,
                             const std::set<std::pair<std::string,std::string>> &snaps);
  int propagate_deleted_entries(const std::string &dir_root,
                                const std::string &epath, const FHandles &fh,
                                std::atomic<bool> &canceled,
                                std::atomic<bool> &failed,
                                std::atomic<int64_t> &op_counter, Context *fin);
  int cleanup_remote_dir(const std::string &dir_root, const std::string &epath,
                         const FHandles &fh, std::atomic<bool> &canceled,
                         std::atomic<bool> &failed);

  void should_sync_entry(const std::string &epath,
                         const struct ceph_statx &cstx,
                         const struct ceph_statx &pstx, bool create_fresh,
                         bool *need_data_sync, uint64_t &change_mask,
                         const FHandles &fh);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root, const Snapshot &current,
                                      boost::optional<Snapshot> prev, FHandles *fh);
  void do_dir_sync(const std::string &dir_root, const std::string &cur_path,
                   const struct ceph_statx &cur_stx, ceph_dir_result *root_dirp,
                   bool create_fresh, const FHandles &fh,
                   std::atomic<bool> &canceled, std::atomic<bool> &failed,
                   std::atomic<int64_t> &op_counter, Context *fin);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);

  void do_dir_sync_using_snapdiff(
      const std::string &dir_root, const std::string &cur_path,
      const struct ceph_statx &cur_stx, ceph_snapdiff_info *snapdiff_info,
      const FHandles &fh, const Snapshot &current, const Snapshot &prev,
      std::atomic<bool> &canceled, std::atomic<bool> &failed,
      std::atomic<int64_t> &op_counter, Context *fin);

  int do_synchronize(const std::string &dir_root, const Snapshot &current);

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);
  int do_sync_snaps(const std::string &dir_root);

  int sync_attributes(const std::string &epath, const struct ceph_statx &stx,
                      uint64_t change_mask, bool is_dir, const FHandles &fh);

  void update_change_mask(const struct ceph_statx &cstx,
                          const struct ceph_statx &pstx, uint64_t &change_mask);

  int remote_mkdir(const std::string &epath, const struct ceph_statx &cstx,
                   const struct ceph_statx &pstx, bool create,
                   const FHandles &fh);
  int remote_file_op(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     bool need_data_sync, uint64_t change_mask,
                     std::atomic<bool> &canceled, std::atomic<bool> &failed,
                     std::atomic<int64_t> &op_counter, Context *fin);
  int sync_attributes_of_file(const std::string &dir_root,
                              const std::string &epath,
                              const struct ceph_statx &stx, const FHandles &fh);
  int copy_to_remote(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     std::atomic<bool> &canceled, std::atomic<bool> &failed);
  void transfer_and_sync_file(const std::string &dir_root,
                              const std::string &epath,
                              const struct ceph_statx &stx, const FHandles &fh,
                              uint64_t change_mask, std::atomic<bool> &canceled,
                              std::atomic<bool> &failed);
  int sync_perms(const std::string &path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
