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
#include "include/frag.h"
#include "include/types.h"
#include "include/utime.h"

#include "Server.h"

class CDir;
class CInode;
class Context;
class MDSContext;
class MDSRank;
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

  // On job id, or on every job if none is given.  fin runs when it is durable.
  int control(Control c, std::optional<uint64_t> id, std::ostream& ss,
              Context *fin);
  void dump(ceph::Formatter *f) const;

private:
  enum class State : uint8_t { RUNNING, PAUSED, DONE, ABORTED };

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
    uint64_t not_auth = 0;   // files and dirfrags on another rank
    uint64_t bytes = 0;
    std::list<Note> head_only_files;  // the latest few
    std::list<Note> errors;           // the latest few
    void add(const Progress& o);
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

    // not persisted
    std::vector<inodeno_t> pending;  // a stack, which keeps the queue small
    std::string hold;                // why nothing starts now
    unsigned failures_in_a_row = 0;

    bool live() const {
      return state == State::RUNNING || state == State::PAUSED;
    }
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
  };

  // A directory being worked on: scanned, then its files migrated.
  struct DirWork {
    Job *job = nullptr;
    inodeno_t ino;
    bool opened = false;
    bool scanned = false;
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
    Progress prog;
  };

  struct FileInFlight {
    uint64_t job;
    utime_t started;
  };

  static std::string job_key(uint64_t id);
  static std::string dir_key(uint64_t id, inodeno_t ino);

  void _load_next(const std::string& after);
  void _load_done();
  void _load_failed(int r);

  void _save_job(const Job& job);
  void _set(const std::string& key, ceph::buffer::list&& bl);
  void _rm(const std::string& key);
  void _flush(Context *fin);
  void _commit();

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
  std::string hold;
};

#endif
