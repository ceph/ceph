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

  struct CommonEntryInfo {
    bool is_dir = false;
    bool purge_remote = false;
    unsigned int change_mask = 0;
    CommonEntryInfo() : is_dir(false), purge_remote(false), change_mask(0) {}
    CommonEntryInfo(bool is_dir, bool purge_remote, unsigned int change_mask)
        : is_dir(is_dir), purge_remote(purge_remote), change_mask(change_mask) {
    }
    CommonEntryInfo(const CommonEntryInfo &other)
        : is_dir(other.is_dir), purge_remote(other.purge_remote),
          change_mask(other.change_mask) {}
    CommonEntryInfo &operator=(const CommonEntryInfo &other) {
      if (this != &other) {
        is_dir = other.is_dir;
        purge_remote = other.purge_remote;
        change_mask = other.change_mask;
      }
      return *this;
    }
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
    int sync_idx;
    struct SyncStat {
      uint64_t rfiles;
      uint64_t rbytes;
      std::atomic<uint64_t> files_in_flight{0};
      std::atomic<uint64_t> files_op[2][2] = {0};
      std::atomic<uint64_t> files_deleted{0};
      std::atomic<uint64_t> large_files_in_flight{0};
      std::atomic<uint64_t> large_files_op[2][2] = {0};
      std::atomic<uint64_t> file_bytes_synced{0};
      std::atomic<uint64_t> dir_created{0};
      std::atomic<uint64_t> dir_deleted{0};
      std::atomic<uint64_t> dir_scanned{0};
      std::atomic<uint64_t> cache_hit{0};
      boost::optional<monotime> start_time;
      SyncStat() : start_time(boost::none) {}
      SyncStat(const SyncStat &other)
          : rfiles(other.rfiles), rbytes(other.rbytes),
            files_in_flight(other.files_in_flight.load()),
            files_deleted(other.files_deleted.load()),
            large_files_in_flight(other.large_files_in_flight.load()),
            file_bytes_synced(other.file_bytes_synced.load()),
            dir_created(other.dir_created.load()),
            dir_deleted(other.dir_deleted.load()),
            dir_scanned(other.dir_scanned.load()),
            cache_hit(other.cache_hit.load()), start_time(other.start_time) {
        for (size_t i = 0; i < 2; ++i) {
          for (size_t j = 0; j < 2; ++j) {
            files_op[i][j].store(other.files_op[i][j].load());
            large_files_op[i][j].store(other.large_files_op[i][j].load());
          }
        }
      }
      SyncStat &operator=(const SyncStat &other) {
        if (this != &other) { // Self-assignment check
          rfiles = other.rfiles;
          rbytes = other.rbytes;
          files_in_flight.store(other.files_in_flight.load());
          files_deleted.store(other.files_deleted.load());
          large_files_in_flight.store(other.large_files_in_flight.load());
          file_bytes_synced.store(other.file_bytes_synced.load());
          dir_created.store(other.dir_created.load());
          dir_deleted.store(other.dir_deleted.load());
          dir_scanned.store(other.dir_scanned.load());
          cache_hit.store(other.cache_hit.load());
          start_time = other.start_time;
          for (size_t i = 0; i < 2; ++i) {
            for (size_t j = 0; j < 2; ++j) {
              files_op[i][j].store(other.files_op[i][j].load());
              large_files_op[i][j].store(other.large_files_op[i][j].load());
            }
          }
        }
        return *this;
      }
      inline void inc_cache_hit() { cache_hit++; }
      inline void inc_file_del_count() { files_deleted++; }
      inline void inc_file_op_count(bool data_synced, bool attr_synced,
                                    uint64_t file_size) {
        files_op[data_synced][attr_synced]++;
        if (file_size >= large_file_threshold) {
          large_files_op[data_synced][attr_synced]++;
        }
        file_bytes_synced.fetch_add(file_size, std::memory_order_relaxed);
      }
      inline void inc_file_in_flight_count(uint64_t file_size) {
        files_in_flight++;
        if (file_size >= large_file_threshold) {
          large_files_in_flight++;
        }
      }
      inline void dec_file_in_flight_count(uint64_t file_size) {
        files_in_flight--;
        if (file_size >= large_file_threshold) {
          large_files_in_flight--;
        }
      }
      inline void inc_dir_created_count() { dir_created++; }
      inline void inc_dir_deleted_count() { dir_deleted++; }
      inline void inc_dir_scanned_count() { dir_scanned++; }
      inline void start_timer() { start_time = clock::now(); }
      void dump(Formatter *f) {
        f->dump_unsigned("rfiles", rfiles);
        f->dump_unsigned("rbytes", rbytes);
        f->dump_unsigned("files_bytes_synced", file_bytes_synced.load());
        if (start_time) {
          std::chrono::duration<double> duration = clock::now() - *start_time;
          double time_elapsed = duration.count();
          f->dump_float("time_elapsed", time_elapsed);
          double speed = 0;
          std::string syncing_speed = "";
          if (time_elapsed > 0) {
            speed = (file_bytes_synced * 8.0) / time_elapsed;
            if (speed >= (double)1e9) {
              syncing_speed = std::to_string(speed / (double)1e9) + "Gbps";
            } else if (speed >= (double)1e6) {
              syncing_speed = std::to_string(speed / (double)1e6) + "Mbps";
            } else if (speed >= (double)1e3) {
              syncing_speed = std::to_string(speed / (double)1e3) + "Kbps";
            } else {
              syncing_speed = std::to_string(speed) + "bps";
            }
            f->dump_string("syncing_speed", syncing_speed);
          }
        }
        f->dump_unsigned("files_in_flight", files_in_flight.load());
        f->dump_unsigned("files_deleted", files_deleted.load());
        f->dump_unsigned("files_data_attr_synced", files_op[1][1].load());
        f->dump_unsigned("files_data_synced", files_op[1][0].load());
        f->dump_unsigned("files_attr_synced", files_op[0][1].load());
        f->dump_unsigned("files_skipped", files_op[0][0].load());
        f->dump_unsigned("large_files_in_flight", large_files_in_flight.load());
        f->dump_unsigned("large_files_data_attr_synced",
                         large_files_op[1][1].load());
        f->dump_unsigned("large_files_data_synced",
                         large_files_op[1][0].load());
        f->dump_unsigned("large_files_attr_synced",
                         large_files_op[0][1].load());
        f->dump_unsigned("large_files_skipped", large_files_op[0][0].load());
        f->dump_unsigned("dir_scanned", dir_scanned);
        f->dump_unsigned("dir_created", dir_created);
        f->dump_unsigned("dir_deleted", dir_deleted);
        f->dump_unsigned("cache_hit", cache_hit);
      }
    };
    SyncStat last_stat, current_stat;
    static const uint64_t large_file_threshold = 4194304;
    SnapSyncStat() {}
    SnapSyncStat(const SnapSyncStat &other)
        : nr_failures(other.nr_failures), last_failed(other.last_failed),
          last_failed_reason(other.last_failed_reason), failed(other.failed),
          last_synced_snap(other.last_synced_snap),
          current_syncing_snap(other.current_syncing_snap),
          synced_snap_count(other.synced_snap_count),
          deleted_snap_count(other.deleted_snap_count),
          renamed_snap_count(other.renamed_snap_count),
          last_synced(other.last_synced),
          last_sync_duration(other.last_sync_duration),
          last_sync_bytes(other.last_sync_bytes), sync_bytes(other.sync_bytes),
          sync_idx(other.sync_idx), last_stat(other.last_stat),
          current_stat(other.current_stat) {}

    SnapSyncStat &operator=(const SnapSyncStat &other) {
      if (this != &other) { // Self-assignment check
        nr_failures = other.nr_failures;
        last_failed = other.last_failed;
        last_failed_reason = other.last_failed_reason;
        failed = other.failed;
        last_synced_snap = other.last_synced_snap;
        current_syncing_snap = other.current_syncing_snap;
        synced_snap_count = other.synced_snap_count;
        deleted_snap_count = other.deleted_snap_count;
        renamed_snap_count = other.renamed_snap_count;
        last_synced = other.last_synced;
        last_sync_duration = other.last_sync_duration;
        last_sync_bytes = other.last_sync_bytes;
        sync_bytes = other.sync_bytes;
        sync_idx = other.sync_idx;
        last_stat = other.last_stat;
        current_stat = other.current_stat;
      }
      return *this;
    }
    void reset_stats() {
      last_stat = current_stat;
      last_stat.start_time = boost::none;
      current_stat = SyncStat();
    }
  };


  class C_MirrorContext;

  class DirOpHandlerContext {
  public:
    class ThreadPool {
    public:
      ThreadPool(int num_threads, int thread_idx, PeerReplayer *replayer)
          : num_threads(num_threads), active(false), task_queue_limit(0),
            queued_task(0), thread_idx(thread_idx), m_peer(replayer->m_peer) {}
      void activate();
      void deactivate();
      bool do_task_async(C_MirrorContext *task);
      void handle_task_force(C_MirrorContext *task);
      bool handle_task_async(C_MirrorContext *task);
      void handle_task_sync(C_MirrorContext *task);
      void update_num_threads(int thread_count);
      friend class PeerReplayer;

    private:
      struct ThreadStatus {
        bool stop_called = true;
        bool active = false;
        ThreadStatus() {}
        ThreadStatus(bool stop_called, bool active)
            : stop_called(stop_called), active(active) {}
      };
      void run_task(ThreadStatus *status);
      void drain_queue();
      int num_threads;
      std::queue<C_MirrorContext *> task_queue;
      std::vector<std::unique_ptr<std::thread>> workers;
      std::vector<ThreadStatus *> thread_status;
      bool active;
      std::condition_variable pick_task;
      std::mutex mtx;
      std::mutex threadpool_config_mutex;
      int task_queue_limit;
      int queued_task = 0;
      int thread_idx;
      Peer& m_peer; // just for using dout
    };
    DirOpHandlerContext(int thread_count, PeerReplayer *replayer)
        : active(true), sync_count(0), thread_count(thread_count),
          replayer(replayer) {}

    std::shared_ptr<ThreadPool> sync_start(int _num_threads);
    void sync_finish(int idx);

    void dump_stats(Formatter *f);

    void deactivate();
    void update_state();

  private:
    std::vector<std::shared_ptr<ThreadPool>> thread_pools;
    std::vector<int> unassigned_sync_ids;
    int sync_count = 0;
    std::mutex context_mutex;
    bool active;
    int thread_count;
    PeerReplayer* replayer; // just for using dout
  };
  DirOpHandlerContext dir_op_handler_context;

  class TaskSinkContext {
  public:
    struct ThreadPoolStats {
      uint64_t total_bytes_queued = 0;
      int large_file_queued = 0;
      int file_queued = 0;
      static const uint64_t large_file_threshold = 4194304;
      TaskSinkContext *task_sink_context;
      void add_file(uint64_t file_size) {
        large_file_queued += (file_size >= large_file_threshold);
        file_queued++;
        total_bytes_queued += file_size;
      }
      void remove_file(uint64_t file_size) {
        large_file_queued -= (file_size >= large_file_threshold);
        file_queued--;
        total_bytes_queued -= file_size;
      }
      ThreadPoolStats() {}
      ThreadPoolStats(int num_threads, TaskSinkContext *task_sink_context)
          : task_sink_context(task_sink_context) {}
      void dump(Formatter *f) {
        f->dump_int("queued_file_count", file_queued);
        f->dump_int("queued_large_file_count", large_file_queued);
        f->dump_unsigned("bytes_queued_for_transfer", total_bytes_queued);
      }
    };

    TaskSinkContext() {}
    TaskSinkContext(int num_threads, PeerReplayer *replayer);
    void activate();
    void deactivate();
    void do_task_async(C_MirrorContext *task);
    // bool do_task_async(C_MirrorContext *task);
    void dump_stats(Formatter *f) {
      // std::scoped_lock lock(fmtx, omtx);
      thread_pool_stats.dump(f);
    }
    int sync_start();
    void sync_finish(int idx);
    void update_state();
    friend class ThreadPoolStats;
    friend class C_TransferAndSyncFile;
    friend class PeerReplayer;

  private:
    void run_task(int idx);
    void drain_queue();
    std::queue<C_MirrorContext *> task_ring;
    std::vector<std::queue<C_MirrorContext *>> task_queue;
    std::condition_variable pick_task;
    std::condition_variable give_task;
    std::mutex mtx;
    std::mutex threadpool_config_mutex;
    int task_limit;
    std::vector<std::thread> workers;
    std::vector<bool> stop_flag;
    ThreadPoolStats thread_pool_stats;
    std::vector<int> unassigned_sync_ids;
    int sync_count = 0;
    bool active;
    PeerReplayer *replayer; // just for using dout
  };
  TaskSinkContext task_sink_context;
  std::condition_variable polling_cv;
  std::thread polling_thread;
  void do_poll();

  class C_MirrorContext : public Context {
  public:
    C_MirrorContext(std::atomic<int64_t> &op_counter, Context *fin,
                    PeerReplayer *replayer,
                    std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : op_counter(op_counter), fin(fin), replayer(replayer),
          m_peer(replayer->m_peer), dir_sync_stat(dir_sync_stat) {}
    virtual void finish(int r) = 0;
    void complete(int r) override {
      finish(r);
      dec_counter();
      delete this;
    }
    void inc_counter() { ++op_counter; }
    virtual void add_into_stat() {}
    virtual void remove_from_stat() {}
    friend class DirOpHandlerContext;
    friend class PeerReplayer;

  protected:
    std::atomic<int64_t> &op_counter;
    Context *fin;
    PeerReplayer *replayer;
    Peer &m_peer; // just for using dout
    std::shared_ptr<SnapSyncStat> &dir_sync_stat;

  private:
    void dec_counter() {
      --op_counter;
      if (op_counter <= 0) {
        fin->complete(0);
      }
    }
  };

  class C_DoDirSync : public C_MirrorContext {
  public:
    C_DoDirSync(const std::string &dir_root, const std::string &cur_path,
                const struct ceph_statx &cstx, ceph_dir_result *dirp,
                bool create_fresh, bool entry_info_known,
                const CommonEntryInfo &entry_info,
                uint64_t common_entry_info_count,
                std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
                const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
                std::atomic<int64_t> &op_counter, Context *fin,
                PeerReplayer *replayer,
                std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : C_MirrorContext(op_counter, fin, replayer, dir_sync_stat),
          dir_root(dir_root), cur_path(cur_path), cstx(cstx), dirp(dirp),
          create_fresh(create_fresh), entry_info_known(entry_info_known),
          entry_info(entry_info),
          common_entry_info_count(common_entry_info_count),
          thread_pool(thread_pool), fh(fh), dir_registry(dir_registry) {}
    void finish(int r) override;
    void set_common_entry_info_count(uint64_t _common_entry_info_count) {
      common_entry_info_count = _common_entry_info_count;
    }

  private:
    const std::string &dir_root;
    std::string cur_path;
    struct ceph_statx cstx;
    ceph_dir_result *dirp;
    bool create_fresh;
    bool entry_info_known;
    CommonEntryInfo entry_info;
    uint64_t common_entry_info_count;
    std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool;
    const FHandles &fh;
    std::shared_ptr<DirRegistry> &dir_registry;
  };

  class C_DoDirSyncSnapDiff : public C_MirrorContext {
  public:
    C_DoDirSyncSnapDiff(
        const std::string &dir_root, const std::string &cur_path,
        ceph_snapdiff_info *sd_info, const Snapshot &current,
        const Snapshot &prev,
        std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
        const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
        std::atomic<int64_t> &op_counter, Context *fin, PeerReplayer *replayer,
        std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : C_MirrorContext(op_counter, fin, replayer, dir_sync_stat),
          dir_root(dir_root), cur_path(cur_path), sd_info(sd_info),
          current(current), prev(prev), thread_pool(thread_pool), fh(fh),
          dir_registry(dir_registry) {}
    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string cur_path;
    ceph_snapdiff_info *sd_info;
    const Snapshot &current;
    const Snapshot &prev;
    std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool;
    const FHandles &fh;
    std::shared_ptr<DirRegistry> &dir_registry;
  };

  class C_TransferAndSyncFile : public C_MirrorContext {
  public:
    C_TransferAndSyncFile(const std::string &dir_root, const std::string &epath,
                          const struct ceph_statx &stx,
                          unsigned int change_mask, const FHandles &fh,
                          std::shared_ptr<DirRegistry> &dir_registry,
                          std::atomic<int64_t> &op_counter, Context *fin,
                          PeerReplayer *replayer,
                          std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : C_MirrorContext(op_counter, fin, replayer, dir_sync_stat),
          dir_root(dir_root), epath(epath), stx(stx), change_mask(change_mask),
          fh(fh), dir_registry(dir_registry) {}

    void finish(int r) override;
    void add_into_stat() override;
    void remove_from_stat() override;
    friend class TaskSinkContext;

  private:
    const std::string &dir_root;
    std::string epath;
    struct ceph_statx stx;
    unsigned int change_mask;
    const FHandles &fh;
    std::shared_ptr<DirRegistry> &dir_registry;
  };

  class C_CleanUpRemoteDir : public C_MirrorContext {
  public:
    C_CleanUpRemoteDir(const std::string &dir_root, const std::string &epath,
                       const FHandles &fh,
                       std::shared_ptr<DirRegistry> &dir_registry,
                       std::atomic<int64_t> &op_counter, Context *fin,
                       PeerReplayer *replayer,
                       std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : C_MirrorContext(op_counter, fin, replayer, dir_sync_stat),
          dir_root(dir_root), epath(epath), fh(fh), dir_registry(dir_registry) {
    }

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    const FHandles &fh;
    std::shared_ptr<DirRegistry> &dir_registry;
  };

  class C_DeleteFile : public C_MirrorContext {
  public:
    C_DeleteFile(const std::string &dir_root, const std::string &epath,
                 const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
                 std::atomic<int64_t> &op_counter, Context *fin,
                 PeerReplayer *replayer,
                 std::shared_ptr<SnapSyncStat> &dir_sync_stat)
        : dir_root(dir_root), epath(epath), fh(fh), dir_registry(dir_registry),
          C_MirrorContext(op_counter, fin, replayer, dir_sync_stat) {}

    void finish(int r) override;

  private:
    const std::string &dir_root;
    std::string epath;
    const FHandles &fh;
    std::shared_ptr<DirRegistry> &dir_registry;
  };

  friend class C_MirrorContext;
  friend class C_DoDirSync;
  friend class C_TransferFile;
  friend class C_CleanUpRemoteDir;
  friend class C_DoDirSyncSnapDiff;
  friend class C_DeleteFile;

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
  bool should_backoff(std::shared_ptr<DirRegistry> &dir_registry, int *retval) {
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
    if (dir_registry->canceled || dir_registry->failed) {
      *retval = -ECANCELED;
      return true;
    }

    *retval = 0;
    return false;
  }

  int get_failed_reason(const std::string &dir_root) {
    std::scoped_lock lock(m_lock);
    auto &dr = m_registered.at(dir_root);
    return dr->failed_reason;
  }

  void mark_failed(const std::string &dir_root, int reason) {
    std::scoped_lock lock(m_lock);
    auto it = m_registered.find(dir_root);
    if (it == m_registered.end()) {
      return;
    }
    if (it->second->failed) {
      return;
    }
    it->second->failed = true;
    it->second->failed_reason = reason;
  }

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  RadosRef m_local_cluster;
  Filesystem m_filesystem;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, std::shared_ptr<DirRegistry>> m_registered;
  std::vector<std::string> m_directories;
  std::map<std::string, std::shared_ptr<SnapSyncStat>> m_snap_sync_stats;
  MountRef m_local_mount;
  ServiceDaemon *m_service_daemon;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
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
  int try_lock_directory(const std::string &dir_root,
                         SnapshotReplayerThread *replayer,
                         std::shared_ptr<DirRegistry> &registry);
  void unlock_directory(const std::string &dir_root, const DirRegistry &registry);
  int sync_snaps(const std::string &dir_root, std::unique_lock<ceph::mutex> &locker);


  int build_snap_map(const std::string &dir_root, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);

  int propagate_snap_deletes(const std::string &dir_root, const std::set<std::string> &snaps);
  int propagate_snap_renames(const std::string &dir_root,
                             const std::set<std::pair<std::string,std::string>> &snaps);

  int delete_file(const std::string &dir_root, const std::string &epath,
                  const FHandles &fh,
                  std::shared_ptr<DirRegistry> &dir_registry,
                  std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  int propagate_deleted_entries(
      const std::string &dir_root, const std::string &epath,
      std::unordered_map<std::string, CommonEntryInfo> &common_entry_info,
      uint64_t &common_entry_info_count, const FHandles &fh,
      std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
      std::shared_ptr<DirRegistry> &dir_registry,
      std::atomic<int64_t> &op_counter, Context *fin,
      std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  int cleanup_remote_dir(const std::string &dir_root, const std::string &epath,
                         const FHandles &fh,
                         std::shared_ptr<DirRegistry> &dir_registry,
                         std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  void should_sync_entry(const std::string &epath,
                         const struct ceph_statx &cstx,
                         const struct ceph_statx &pstx, bool create_fresh,
                         bool *need_data_sync, uint64_t &change_mask,
                         const FHandles &fh);

  int open_dir(MountRef mnt, const std::string &dir_path, boost::optional<uint64_t> snap_id);
  int pre_sync_check_and_open_handles(const std::string &dir_root, const Snapshot &current,
                                      boost::optional<Snapshot> prev, FHandles *fh);

  void build_change_mask(const struct ceph_statx &pstx,
                         const struct ceph_statx &cstx, bool create_fresh,
                         unsigned int &change_mask);

  void
  do_dir_sync(const std::string &dir_root, const std::string &cur_path,
              const struct ceph_statx &cstx, ceph_dir_result *dirp,
              bool create_fresh, bool stat_known, CommonEntryInfo &entry_info,
              uint64_t common_entry_info_count,
              std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
              const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
              std::atomic<int64_t> &op_counter, Context *fin,
              std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  int do_synchronize(const std::string &dir_root, const Snapshot &current,
                     boost::optional<Snapshot> prev);

  int handle_duplicate_entry(
      const std::string &dir_root, const std::string &cur_path,
      const ceph_snapdiff_entry_t &sd_entry,
      std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
      const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
      std::atomic<int64_t> &op_counter, Context *fin,
      std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  void do_dir_sync_using_snapdiff(
      const std::string &dir_root, const std::string &cur_path,
      ceph_snapdiff_info *sd_info, const Snapshot &current,
      const Snapshot &prev,
      std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
      const FHandles &fh, std::shared_ptr<DirRegistry> &dir_registry,
      std::atomic<int64_t> &op_counter, Context *fin,
      std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  int do_synchronize(const std::string &dir_root, const Snapshot &current);

  int synchronize(const std::string &dir_root, const Snapshot &current,
                  boost::optional<Snapshot> prev);

  int _do_sync_snaps(const std::string &dir_root, uint64_t cur_snap_id,
                     std::string cur_snap_name, uint64_t last_snap_id,
                     std::string last_snap_name);

  int do_sync_snaps(const std::string &dir_root);

  int _remote_mkdir(const std::string &epath, const struct ceph_statx &cstx,
                    const FHandles &fh);

  int remote_mkdir(const std::string &epath, const struct ceph_statx &cstx,
                   bool create_fresh, unsigned int change_mask,
                   const FHandles &fh,
                   std::shared_ptr<SnapSyncStat> &dir_sync_stat);

  int remote_file_op(
      const std::string &dir_root, const std::string &epath,
      const struct ceph_statx &stx, bool need_data_sync,
      unsigned int change_mask, const FHandles &fh,
      std::shared_ptr<DirOpHandlerContext::ThreadPool> &thread_pool,
      std::shared_ptr<DirRegistry> &dir_registry,
      std::atomic<int64_t> &op_counter, Context *fin,
      std::shared_ptr<SnapSyncStat> &dir_sync_stat);
  int sync_attributes(const std::string &epath, const struct ceph_statx &stx,
                      unsigned int change_mask, bool is_dir,
                      const FHandles &fh);
  int copy_to_remote(const std::string &dir_root, const std::string &epath,
                     const struct ceph_statx &stx, const FHandles &fh,
                     std::shared_ptr<DirRegistry> &dir_registry);
  void transfer_and_sync_file(const std::string &dir_root,
                              const std::string &epath,
                              const struct ceph_statx &stx,
                              unsigned int change_mask, const FHandles &fh,
                              std::shared_ptr<DirRegistry> &dir_registry,
                              std::shared_ptr<SnapSyncStat> &dir_sync_stat);
  int sync_perms(const std::string &path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H