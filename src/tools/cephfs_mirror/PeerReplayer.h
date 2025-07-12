// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_PEER_REPLAYER_H
#define CEPHFS_MIRROR_PEER_REPLAYER_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "ServiceDaemon.h"
#include "Types.h"
#include "common/Cond.h"

#include <stack>
#include <boost/optional.hpp>

namespace cephfs {
namespace mirror {

class FSMirror;
class PeerReplayerAdminSocketHook;

class PeerReplayer : public md_config_obs_t {
public:
  PeerReplayer(CephContext *cct, FSMirror *fs_mirror,
               RadosRef local_cluster, const Filesystem &filesystem,
               const Peer &peer, const std::set<std::string, std::less<>> &directories,
               MountRef mount, ServiceDaemon *service_daemon);
  ~PeerReplayer();
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy &conf,
                          const std::set<std::string> &changed) override;

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

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  inline static const std::string SERVICE_DAEMON_FAILED_DIR_COUNT_KEY = "failure_count";
  inline static const std::string SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY = "recovery_count";

  using Snapshot = std::pair<std::string, uint64_t>;

  // file descriptor "triplet" for synchronizing a snapshot
  // w/ an added MountRef for accessing "previous" snapshot.
  struct FHandles {
    // open file descriptor on the snap directory for snapshot
    // currently being synchronized. Always use this fd with
    // @m_local_mount.
    int c_fd = -1;

    // open file descriptor on the "previous" snapshot or on
    // dir_root on remote filesystem (based on if the snapshot
    // can be used for incremental transfer). Always use this
    // fd with p_mnt which either points to @m_local_mount (
    // for local incremental comparison) or @m_remote_mount (
    // for remote incremental comparison).
    int p_fd = -1;
    MountRef p_mnt;

    // open file descriptor on dir_root on remote filesystem.
    // Always use this fd with @m_remote_mount.
    int r_fd_dir_root = -1;
    Snapshot m_current;
    boost::optional<Snapshot> m_prev;
  };

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

  class DirSyncPool;
  struct DirRegistry {
    int fd;
    std::atomic<bool> canceled = false;
    std::atomic<bool> failed = false;
    SnapshotReplayerThread *replayer;
    std::string dir_root;
    FHandles snapdiff_fh, remotediff_fh;
    std::atomic<int> failed_reason = 0;
    int file_sync_queue_idx = -1;
    std::unique_ptr<C_SaferCond> sync_finish_cond;
    std::atomic <int> sync_indicator = 0;
    std::unique_ptr <DirSyncPool> sync_pool = nullptr;
    void set_failed(int r) {
      ceph_assert(r < 0);
      if (failed) {
        return;
      }
      failed.store(true);
      failed_reason.store(r);
    }
    void sync_start(int _file_sync_queue_idx) {
      sync_indicator = 0;
      file_sync_queue_idx = _file_sync_queue_idx;
      sync_finish_cond = std::make_unique<C_SaferCond>();
    }
    void wait_for_sync_finish() {
      sync_finish_cond->wait();
    }
    int get_file_sync_queue_idx() {
      return file_sync_queue_idx;
    }
    void inc_sync_indicator() {
      ++sync_indicator;
    }
    void dec_sync_indicator() {
      --sync_indicator;
      if (sync_indicator <= 0) {
        sync_finish_cond->complete(0);
      }
    }
  };

  struct SyncEntry {
    static const unsigned int PURGE_REMOTE = (1 << 20);
    static const unsigned int CREATE_FRESH = (1 << 21);
    static const unsigned int WASNT_DIR_IN_PREV_SNAPSHOT = (1 << 22);
    static const unsigned int CHANGE_MASK_POPULATED = (1 << 23);
    std::string epath;
    ceph_dir_result *dirp = nullptr; // valid for directories
    ceph_snapdiff_info info;
    struct ceph_statx stx;
    // set by incremental sync _after_ ensuring missing entries
    // in the currently synced snapshot have been propagated to
    // the remote filesystem.
    bool remote_synced = false;

    unsigned int change_mask = 0;
    bool is_snapdiff = false;
    ceph_snapdiff_entry_t deleted_sibling;
    bool deleted_sibling_exist = false;
    bool stat_known = false;

    SyncEntry() {
    }

    SyncEntry(std::string_view path) : epath(path) {}

    SyncEntry(std::string_view path,
              const struct ceph_statx &stx)
      : epath(path),
        stx(stx) {
    }
    SyncEntry(std::string_view path,
              ceph_dir_result *dirp,
              const struct ceph_statx &stx)
      : epath(path),
        dirp(dirp),
        stx(stx) {
    }
    SyncEntry(std::string_view path, ceph_dir_result *dirp,
              const struct ceph_statx &stx, unsigned int change_mask)
        : epath(path), dirp(dirp), stx(stx), change_mask(change_mask) {}
    SyncEntry(std::string_view path, const ceph_snapdiff_info &info,
              const struct ceph_statx &stx, unsigned int change_mask)
        : epath(path), info(info), stx(stx), change_mask(change_mask) {
      is_snapdiff = true;
    }

    bool is_directory() const {
      return S_ISDIR(stx.stx_mode);
    }

    bool needs_remote_sync() const {
      return !remote_synced;
    }
    void set_remote_synced() {
      remote_synced = true;
    }

    void set_is_snapdiff(bool f) { 
      is_snapdiff = f; 
    }

    bool sync_is_snapdiff() const {
      return is_snapdiff;
    }

    void set_stat_known() {
      stat_known = true;
    }

    void reset_change_mask() {
      change_mask = 0;
    }

    bool change_mask_populated() {
      return (change_mask & CHANGE_MASK_POPULATED);
    }

    bool purge_remote() {
      return (change_mask & PURGE_REMOTE);
    }
    bool create_fresh() {
      return (change_mask & CREATE_FRESH);
    }
    bool wasnt_dir_in_prev_snapshot() {
      return (change_mask & WASNT_DIR_IN_PREV_SNAPSHOT);
    }
  };
  class FileSyncMechanism;
  class FileSyncPool {
  public:
    struct PoolStats {
      uint64_t total_bytes_queued = 0;
      int file_queued = 0;
      FileSyncPool *pool;
      void add_file(uint64_t file_size) {
        file_queued++;
        total_bytes_queued += file_size;
      }
      void remove_file(uint64_t file_size) {
        file_queued--;
        total_bytes_queued -= file_size;
      }
      PoolStats() {}
      PoolStats(FileSyncPool *pool) : pool(pool) {}
      void dump(Formatter *f) {
        f->dump_int("queued_file_count", file_queued);
        f->dump_unsigned("bytes_queued_for_transfer", total_bytes_queued);
      }
    };

    FileSyncPool(int num_thread, const Peer& m_peer);
    void activate();
    void deactivate();
    void sync_file_data(FileSyncMechanism *task);
    // bool do_task_async(C_MirrorContext *task);
    void dump_stats(Formatter *f) {
      std::scoped_lock lock(mtx);
      pool_stats.dump(f);
    }
    int sync_start();
    void sync_finish(int idx);
    void update_state();
    friend class PoolStats;

  private:
    void run(int idx);
    void drain_queue();
    int num_threads;
    std::queue<FileSyncMechanism *> sync_ring;
    std::vector<std::queue<FileSyncMechanism *>> sync_queue;
    std::condition_variable pick_cv;
    std::condition_variable give_cv;
    std::mutex mtx;
    int qlimit;
    std::vector<std::unique_ptr<std::thread>> workers;
    std::vector<bool> stop_thread;
    PoolStats pool_stats;
    std::vector<int> unassigned_sync_ids;
    int sync_count = 0;
    bool active;
    const Peer& m_peer; // for dout
  };
  FileSyncPool file_sync_pool;

  class SyncMechanism;
  class DirSyncPool {
  public:
    DirSyncPool(int num_threads, const std::string epath, const Peer &m_peer)
        : num_threads(num_threads), active(false), queued(0), qlimit(0),
          epath(epath), m_peer(m_peer) {}
    void activate();
    void deactivate();
    bool try_sync(SyncMechanism *task);
    void sync_anyway(SyncMechanism *task);
    void sync_direct(SyncMechanism *task);
    void update_state();
    void dump_stats(Formatter *f) {
      std::scoped_lock lock(mtx);
      f->dump_unsigned("dir_sync_queue_size", sync_queue.size());
    }
    friend class PeerReplayer;

  private:
    struct ThreadStatus {
      bool stop_called = true;
      bool active = false;
      ThreadStatus() {}
      ThreadStatus(bool stop_called, bool active)
          : stop_called(stop_called), active(active) {}
    };
    void run(ThreadStatus *status);
    void drain_queue();
    bool _try_sync(SyncMechanism *task);
    int num_threads;
    std::queue<SyncMechanism *> sync_queue;
    std::vector<std::unique_ptr<std::thread>> workers;
    std::vector<ThreadStatus *> thread_status;
    bool active;
    std::condition_variable pick_cv;
    std::mutex mtx;
    int queued = 0;
    int qlimit;
    std::string epath;
    const Peer &m_peer; // just for using dout
  };

  struct SnapSyncStat;

  class SyncMechanism : public Context {
  public:
    SyncMechanism(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                  DirRegistry *registry, SnapSyncStat *sync_stat,
                  const FHandles &fh, PeerReplayer *replayer)
        : m_local(m_local), m_peer(replayer->m_peer),
          cur_entry(std::move(cur_entry)), registry(registry),
          sync_stat(sync_stat), fh(fh), replayer(replayer) {}

    void sync_in_flight() { registry->inc_sync_indicator(); }
    std::shared_ptr<SyncEntry> &&move_cur_entry() {
      return std::move(cur_entry);
    }

  protected:
    MountRef m_local;
    Peer &m_peer;
    std::shared_ptr<SyncEntry> cur_entry;
    DirRegistry *registry;
    SnapSyncStat *sync_stat;
    const FHandles &fh;
    PeerReplayer *replayer;
    int populate_change_mask(const FHandles &fh);
    int populate_current_stat(const FHandles& fh);
  };

  class DeleteMechanism : public SyncMechanism {
  public:
    DeleteMechanism(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                    DirRegistry *registry, SnapSyncStat *sync_stat,
                    const FHandles &fh, PeerReplayer *replayer,
                    int not_dir = -1)
        : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                        replayer),
          not_dir(not_dir) {}

  private:
    void finish(int r) override;
    int not_dir = -1;
  };

  class FileSyncMechanism : public SyncMechanism {
  public:
    FileSyncMechanism(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                      DirRegistry *registry, SnapSyncStat *sync_stat,
                      const FHandles &fh, PeerReplayer *replayer)
        : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                        replayer) {}

    uint64_t get_file_size() { return cur_entry->stx.stx_size; }
    int get_file_sync_queue_idx() {
      return registry->get_file_sync_queue_idx();
    }

  private:
    void finish(int r) override;
    int sync_file();
    int _sync_file();
  };

  class DirSyncMechanism : public SyncMechanism {
  public:
    DirSyncMechanism(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                     DirRegistry *registry, SnapSyncStat *sync_stat,
                     const FHandles &fh, PeerReplayer *replayer)
        : SyncMechanism(m_local, std::move(cur_entry), registry, sync_stat, fh,
                        replayer) {}

  protected:
    void finish(int r) override;
    int sync_tree();
    bool try_spawning(SyncMechanism* syncm);
    std::stack<std::shared_ptr<SyncEntry>> m_sync_stack;

  private:
    virtual void finish_sync() = 0;
    virtual int go_next() = 0;
    virtual int sync_current_entry() = 0;
  };

  static const int max_change_mask_map_size = 1e5;
  class DirBruteDiffSync : public DirSyncMechanism {
  public:
    DirBruteDiffSync(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                     DirRegistry *registry, SnapSyncStat *sync_stat,
                     const FHandles &fh, PeerReplayer *replayer)
        : DirSyncMechanism(m_local, std::move(cur_entry), registry, sync_stat,
                           fh, replayer) {}

  private:
    std::stack<std::unordered_map<std::string, unsigned int>>
        m_change_mask_map_stack;
    int change_mask_map_size = 0;
    void finish_sync() override;
    int go_next() override;
    int sync_current_entry() override;
  };

  class DirSnapDiffSync : public DirSyncMechanism {
  public:
    DirSnapDiffSync(MountRef m_local, std::shared_ptr<SyncEntry> &&cur_entry,
                    DirRegistry *registry, SnapSyncStat *sync_stat,
                    const FHandles &fh, PeerReplayer *replayer)
        : DirSyncMechanism(m_local, std::move(cur_entry), registry, sync_stat,
                           fh, replayer) {}

  private:
    void finish_sync() override;
    int go_next() override;
    int sync_current_entry() override;
    bool should_delete_current_entry(int not_dir = -1);
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
    struct SyncStat {
      std::atomic<uint64_t> files_in_flight{0};
      std::atomic<uint64_t> files_synced{0};
      std::atomic<uint64_t> files_deleted{0};
      std::atomic<uint64_t> file_bytes_synced{0};
      std::atomic<uint64_t> dir_created{0};
      std::atomic<uint64_t> dir_deleted{0};
      std::atomic<uint64_t> dir_scanned{0};
      boost::optional<monotime> start_time;
      SyncStat() : start_time(boost::none) {}
      SyncStat(const SyncStat &other)
          : files_in_flight(other.files_in_flight.load()),
            files_synced(other.files_synced.load()),
            files_deleted(other.files_deleted.load()),
            file_bytes_synced(other.file_bytes_synced.load()),
            dir_created(other.dir_created.load()),
            dir_deleted(other.dir_deleted.load()),
            dir_scanned(other.dir_scanned.load()) {}
      SyncStat &operator=(const SyncStat &other) {
        if (this != &other) { // Self-assignment check
          files_in_flight.store(other.files_in_flight.load());
          files_synced.store(other.files_synced.load());
          files_deleted.store(other.files_deleted.load());
          file_bytes_synced.store(other.file_bytes_synced.load());
          dir_created.store(other.dir_created.load());
          dir_deleted.store(other.dir_deleted.load());
          dir_scanned.store(other.dir_scanned.load());
        }
        return *this;
      }
      inline void inc_file_del_count() { files_deleted++; }
      inline void inc_files_synced(bool data_synced, uint64_t file_size) {
        files_synced++;
        if (data_synced) {
          file_bytes_synced += file_size;
        }
      }
      inline void inc_file_in_flight_count() { files_in_flight++; }
      inline void dec_file_in_flight_count() { files_in_flight--; }
      inline void inc_dir_created_count() { dir_created++; }
      inline void inc_dir_deleted_count() { dir_deleted++; }
      inline void inc_dir_scanned_count() { dir_scanned++; }
      inline void start_timer() { start_time = clock::now(); }
      void dump(Formatter *f) {
        f->dump_unsigned("files_in_flight", files_in_flight.load());
        f->dump_unsigned("files_synced", files_synced.load());
        f->dump_unsigned("files_deleted", files_deleted.load());
        f->dump_unsigned("files_bytes_synced", file_bytes_synced.load());
        f->dump_unsigned("dir_created", dir_created);
        f->dump_unsigned("dir_scanned", dir_scanned);
        f->dump_unsigned("dir_deleted", dir_deleted);
        if (start_time) {
          std::chrono::duration<double> duration = clock::now() - *start_time;
          double time_elapsed = duration.count();
          f->dump_float("time_elapsed", time_elapsed);
          double rate = 0;
          std::string transfer_rate = "";
          if (time_elapsed > 0) {
            rate = file_bytes_synced / time_elapsed;
            if (rate >= (double)1e9) {
              transfer_rate = std::to_string(rate / (double)1e9) + " GB/s";
            } else if (rate >= (double)1e6) {
              transfer_rate = std::to_string(rate / (double)1e6) + " MB/s";
            } else if (rate >= (double)1e3) {
              transfer_rate = std::to_string(rate / (double)1e3) + " KB/s";
            } else {
              transfer_rate = std::to_string(rate) + " B/s";
            }
            f->dump_string("transfer-rate", transfer_rate);
          }
        }
      }
    };
    SyncStat last_stat, current_stat;
    void reset_stats() {
      last_stat = current_stat;
      last_stat.start_time = boost::none;
      current_stat = SyncStat();
    }
  };

  void _inc_failed_count(const std::string &dir_root) {
    auto max_failures = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_consecutive_failures_per_directory");
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->last_failed = clock::now();
    if (++sync_stat->nr_failures >= max_failures && !sync_stat->failed) {
      sync_stat->failed = true;
      ++m_service_daemon_stats.failed_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_FAILED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.failed_dir_count);
    }
  }
  void _reset_failed_count(const std::string &dir_root) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    if (sync_stat->failed) {
      ++m_service_daemon_stats.recovered_dir_count;
      m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                     SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY,
                                                     m_service_daemon_stats.recovered_dir_count);
    }
    sync_stat->nr_failures = 0;
    sync_stat->failed = false;
    sync_stat->last_failed = boost::none;
    sync_stat->last_failed_reason = boost::none;
  }

  void _set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->last_synced_snap = std::make_pair(snap_id, snap_name);
    sync_stat->current_syncing_snap = boost::none;
  }
  void set_last_synced_snap(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->sync_bytes = 0;
  }
  void set_current_syncing_snap(const std::string &dir_root, uint64_t snap_id,
                                const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->current_syncing_snap = std::make_pair(snap_id, snap_name);
  }
  void clear_current_syncing_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->current_syncing_snap = boost::none;
  }
  void inc_deleted_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat->deleted_snap_count;
  }
  void inc_renamed_snap(const std::string &dir_root) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    ++sync_stat->renamed_snap_count;
  }
  void set_last_synced_stat(const std::string &dir_root, uint64_t snap_id,
                            const std::string &snap_name, double duration) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_root, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->last_synced = clock::now();
    sync_stat->last_sync_duration = duration;
    sync_stat->last_sync_bytes = sync_stat->sync_bytes;
    ++sync_stat->synced_snap_count;
  }
  void inc_sync_bytes(const std::string &dir_root, const uint64_t& b) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    sync_stat->sync_bytes += b;
  }
  bool should_backoff(DirRegistry* registry, int *retval) {
    if (m_fs_mirror->is_blocklisted()) {
      *retval = -EBLOCKLISTED;
      return true;
    }

    if (is_stopping()) {
      // ceph defines EBLOCKLISTED to ESHUTDOWN (108). so use
      // EINPROGRESS to identify shutdown.
      *retval = -EINPROGRESS;
      return true;
    }
    if (registry->failed || registry->canceled) {
      *retval = -ECANCELED;
      return true;
    }

    *retval = 0;
    return false;
  }

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  RadosRef m_local_cluster;
  Filesystem m_filesystem;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry*> m_registered;
  std::vector<std::string> m_directories;
  std::map<std::string, SnapSyncStat *> m_snap_sync_stats;
  MountRef m_local_mount;
  ServiceDaemon *m_service_daemon;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
  ceph::condition_variable m_cond;
  RadosRef m_remote_cluster;
  MountRef m_remote_mount;
  bool m_stopping = false;
  SnapshotReplayers m_replayers;

  ServiceDaemonStats m_service_daemon_stats;

  PerfCounters *m_perf_counters;

  void run(SnapshotReplayerThread *replayer);

  boost::optional<std::string> pick_directory();
  int register_directory(const std::string &dir_root, SnapshotReplayerThread *replayer);
  void unregister_directory(const std::string &dir_root);
  int try_lock_directory(const std::string &dir_root, SnapshotReplayerThread *replayer,
                         DirRegistry *registry);
  void unlock_directory(const std::string &dir_root, DirRegistry* registry);
  int sync_snaps(const std::string &dir_root, std::unique_lock<ceph::mutex> &locker);


  int build_snap_map(const std::string &dir_root, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);

  int propagate_snap_deletes(const std::string &dir_root, const std::set<std::string> &snaps);
  int propagate_snap_renames(const std::string &dir_root,
                             const std::set<std::pair<std::string,std::string>> &snaps);
  int propagate_deleted_entries(
      const std::string &epath, DirRegistry *registry, SnapSyncStat *sync_stat,
      const FHandles &fh,
      std::unordered_map<std::string, unsigned int> &change_mask_map,
      int &change_mask_map_size);
  int cleanup_remote_entry(const std::string &epath, DirRegistry *registry,
                           const FHandles &fh, SnapSyncStat *sync_stat,
                           int not_dir = -1);

  int should_sync_entry(const std::string &epath, const struct ceph_statx &cstx,
                        const FHandles &fh, bool *need_data_sync, bool *need_attr_sync);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root,
                                      const Snapshot &current,
                                      boost::optional<Snapshot> prev,
                                      FHandles *snapdiff_fh,
                                      FHandles *remotediff_fh);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);
  int do_synchronize(const std::string &dir_root, const Snapshot &current) {
    return do_synchronize(dir_root, current, boost::none);
  }

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);
  int do_sync_snaps(const std::string &dir_root);

  int sync_attributes(std::shared_ptr<SyncEntry> &cur_entry, const FHandles &fh);

  int remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry, const FHandles &fh,
                   SnapSyncStat *sync_stat);
  int _remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry, const FHandles &fh,
                    SnapSyncStat *sync_stat);
  int remote_file_op(std::shared_ptr<SyncEntry> &cur_entry,
                     DirRegistry *registry, SnapSyncStat *sync_stat,
                     const FHandles &fh);
  int copy_to_remote(std::shared_ptr<SyncEntry> &cur_entry,
                     DirRegistry *registry, const FHandles &fh,
                     uint64_t num_blocks, struct cblock *b, bool trunc = false);
  int sync_perms(const std::string& path);
  void build_change_mask(const struct ceph_statx &pstx,
                         const struct ceph_statx &cstx, bool create_fresh,
                         bool purge_remote, unsigned int &change_mask);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
