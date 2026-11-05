// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDS_MIGRATEQUEUE_H
#define CEPH_MDS_MIGRATEQUEUE_H

#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/cephfs/types.h"
#include "include/frag.h"
#include "include/types.h"
#include "include/utime.h"
#include "msg/MessageRef.h"

#include "Server.h"

class CDir;
class CInode;
class Context;
class MDSContext;
class MDSRank;
class Message;
class MMDSMigrate;
namespace ceph { class Formatter; }

/*
 * Background migration of directory trees to another data pool, started with
 * ceph.dir.migrate.pool.  See MigrateQueue.cc.  All of it runs under mds_lock.
 */
class MigrateQueue {
public:
  enum class Control { PAUSE, RESUME, ABORT };

  explicit MigrateQueue(MDSRank *mds);
  ~MigrateQueue();

  // Read what a previous MDS left; called when the rank has recovered.
  void load();
  bool is_ready() const { return ready; }
  void wait_for_ready(MDSContext *c);

  /*
   * Start migrating all the regular files below dir, a directory this rank is
   * auth for.  fin gets 0 once the job is durable.  Asking again for a job
   * that is under way is not an error; a clash with one gives -EBUSY.
   */
  void enqueue(CInode *dir, int64_t pool, Context *fin);

  void tick();

  // A stopping rank gives its sub-jobs back; it may stop once this is true.
  bool drained(std::ostream& ss);

  // Messages from the other ranks; each is resent until it has had an effect.
  void dispatch(const cref_t<Message>& m);

  // On job id, or on every job if none is given.  fin runs when it is durable.
  int control(Control c, std::optional<uint64_t> id, std::ostream& ss,
              Context *fin);
  void dump(ceph::Formatter *f) const;

private:
  enum class State : uint8_t { RUNNING, PAUSED, DONE, ABORTED };

  // Something to walk: a directory, or part of one, or a single file.
  struct Item {
    inodeno_t ino;
    frag_t fg;          // the root frag stands for the whole directory
    bool file = false;
    mds_rank_t where = MDS_RANK_NONE;  // who holds it, as far as we know
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  struct Note {
    inodeno_t ino;
    std::string path;
    int32_t err = 0;
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
  };

  // Counted per directory, and added to the job in the same write that takes
  // the directory off the queue, so a directory interrupted by a failover and
  // walked again is counted once.
  struct Progress {
    uint64_t dirs = 0;       // directories finished
    uint64_t moved = 0;      // files moved, head_only ones included
    uint64_t head_only = 0;  // moved, but their snapshots stayed behind
    uint64_t already = 0;    // links to a file that was in the pool already
    uint64_t failed = 0;
    uint64_t gone = 0;       // deleted before we got to them
    uint64_t other = 0;      // symlinks and the like
    uint64_t not_auth = 0;   // left behind on another rank by an abort
    uint64_t bytes = 0;
    std::list<Note> head_only_files;  // the latest few
    std::list<Note> errors;           // the latest few
    void add(const Progress& o);
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  // What the sub-job on another rank last said about itself.
  struct Report {
    uint64_t job = 0;           // its id on that rank
    uint8_t state = 0;
    std::string reason;
    std::string hold;
    uint64_t accepted = 0;      // the last item it took from the origin
    bool idle = false;          // durably nothing left, all it found handed in
    uint64_t dirs_active = 0;
    uint64_t dirs_queued = 0;
    uint64_t files_in_progress = 0;
    uint64_t control_seq = 0;   // the last pause or resume it applied
    Progress total;
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  /*
   * One direction of hand-overs and the other, between a job and its sub-job
   * on another rank.  Items get a number per link and sit in the queue object
   * as hand.<id>.<rank>.<seq> until the peer has them durably.
   */
  struct Link {
    mds_rank_t rank = MDS_RANK_NONE;
    uint64_t sent = 0;          // the last number given out
    uint64_t acked = 0;         // what the peer has durably, cumulative
    uint64_t accepted = 0;      // the last number taken from the peer
    Report report;              // origin only

    // not persisted
    std::map<uint64_t, Item> out;  // numbered sent back to acked
    uint64_t durable = 0;       // only what we would remember goes out
    uint64_t on_wire = 0;
    utime_t last_sent;
    std::string refused;        // why the peer does not take them
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  struct Job {
    uint64_t id = 0;
    inodeno_t root;
    std::string path;
    int64_t pool = -1;
    std::string pool_name;
    State state = State::RUNNING;
    std::string reason;
    utime_t started, finished;
    Progress done;

    // A sub-job does on this rank a part of a job started on another: the
    // origin.  Everything it finds on a third rank goes through the origin.
    mds_rank_t origin_rank = MDS_RANK_NONE;
    uint64_t origin_job = 0;
    bool over = false;          // the origin told us it is finished with it
    // Counts the operator's pauses and resumes.  A sub-job pausing on its own
    // after failures is not to be undone by the origin wanting it to run.
    uint64_t control_seq = 0;
    std::map<mds_rank_t, Link> links;

    // not persisted
    std::vector<Item> pending;       // a stack, which keeps the queue small
    std::string hold;                // why nothing starts now
    unsigned failures_in_a_row = 0;
    uint64_t idle_seen = 0;      // the origin wrote down we were idle at this

    bool live() const {
      return state == State::RUNNING || state == State::PAUSED;
    }
    bool is_sub() const { return origin_rank != MDS_RANK_NONE; }
    // A sub-job with nothing to do can get more until the origin is finished.
    bool open() const { return live() || (is_sub() && !over); }
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  // A directory being worked on: scanned, then its files migrated.
  struct DirWork {
    Job *job = nullptr;
    Item item;
    bool opened = false;
    bool scanned = false;
    bool redo = false;          // this rank stops: someone else must do it
    std::vector<frag_t> frags_done;
    std::vector<inodeno_t> files;
    size_t next = 0;
    unsigned in_flight = 0;
    struct Retry {
      inodeno_t ino;
      unsigned tries;
      utime_t when;
    };
    std::list<Retry> retries;
    std::set<inodeno_t> subdirs;
    std::vector<Item> remote;   // found on other ranks
    Progress prog;
  };

  struct FileInFlight {
    uint64_t job;
    utime_t started;
  };

  static std::string job_key(uint64_t id);
  static std::string dir_key(uint64_t id, const Item& item);
  static std::string hand_key(uint64_t id, mds_rank_t rank, uint64_t seq);

  void _load_next(const std::string& after);
  void _load_done();
  void _load_failed(int r);

  void _save_job(const Job& job);
  void _queue_item(Job& job, const Item& item);
  void _set(const std::string& key, ceph::buffer::list&& bl);
  void _rm(const std::string& key);
  void _flush(Context *fin);
  void _commit();

  Job *_find_sub(mds_rank_t origin, uint64_t id);
  Link& _link(Job& job, mds_rank_t rank);
  bool _settled(const Job& job, const Link& l) const;
  void _route(Job& job, const Item& item);
  void _hand_over(Job& job, mds_rank_t to, const Item& item);
  Context *_on_durable(const Job& job);
  void _send_items(Job& job, Link& l, bool resend);
  void _send_msg(int op, mds_rank_t to, mds_rank_t origin, uint64_t id,
                 ceph::buffer::list&& bl);
  void _send_ack(mds_rank_t to, mds_rank_t origin, uint64_t id,
                 uint64_t accepted, int err, const std::string& reason);
  void _report(Job& job);
  void _tell_rank(uint64_t id, mds_rank_t rank, uint64_t seen);
  void _tell_ranks(const Job& job);
  void _tick_links();
  void _give_back();
  void _check_gone_ranks(Job& job);
  std::string _clash(const std::string& path, int64_t pool) const;
  void _handle_items(mds_rank_t from, const cref_t<MMDSMigrate>& m);
  void _handle_ack(mds_rank_t from, const cref_t<MMDSMigrate>& m);
  void _handle_report(mds_rank_t from, const cref_t<MMDSMigrate>& m);
  void _handle_control(const cref_t<MMDSMigrate>& m);
  Progress _total(const Job& job, uint64_t *dirs_active = nullptr,
                  uint64_t *in_flight = nullptr) const;

  void _kick(bool tick = false);
  bool _start_something();
  std::string _global_hold() const;
  void _check_job(Job& job);
  void _start_dir(Job& job);
  void _scan_dir(DirWork *dw);
  void _scan_frag(DirWork *dw, CDir *dir);
  void _start_file(DirWork *dw, inodeno_t ino, unsigned tries, bool opened);
  void _file_migrated(DirWork *dw, inodeno_t ino, unsigned tries, int r,
                      const Server::FileMigrateResult& res);
  void _wait_cleanup(DirWork *dw, inodeno_t ino);
  void _slot_done(DirWork *dw);
  void _maybe_finish_dir(DirWork *dw);
  void _maybe_finish_job(Job& job);
  void _abort_job(Job& job, const std::string& reason);
  void _trim_jobs();
  MDSContext *_scan_again(DirWork *dw);
  void _note(std::list<Note>& l, inodeno_t ino, int err);

  MDSRank *mds;
  std::string oid;

  bool loading = false;
  bool ready = false;
  bool load_failed = false;
  std::vector<MDSContext*> ready_waiters;
  std::map<std::string, ceph::buffer::list> loaded;

  std::map<std::string, ceph::buffer::list> to_set;
  std::set<std::string> to_rm;
  std::vector<Context*> to_finish;
  bool committing = false;

  std::map<uint64_t, Job> jobs;
  uint64_t last_id = 0;
  std::list<std::unique_ptr<DirWork>> active;
  std::map<inodeno_t, FileInFlight> files_in_flight;
  unsigned slots = 0;   // files being worked on, or looked up
  bool kicking = false;
  bool warned_stopping = false;
  std::string hold;
};

#endif
