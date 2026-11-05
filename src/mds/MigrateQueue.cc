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

/*
 * ceph.dir.migrate.pool: move every regular file below a directory to another
 * data pool, in the background.
 *
 * A job walks the tree the way scrub does, through the cache, and migrates
 * each file with Server::file_migrate().  What makes it survive a failover is
 * a work list in one omap object of the metadata pool, mds<rank>_migratequeue:
 *
 *   job.<id>         the job: root, target pool, state, counters
 *   dir.<id>.<ino>   a directory with work left in it
 *
 * Only directories are recorded, never files.  A directory stays listed until
 * every file in it has been moved and its old pool cleared.  Then one write
 * removes it, adds its subdirectories and updates the job's counters.  A
 * failover therefore repeats at most the directories that were in progress,
 * and repeating one is cheap and safe: a file already in the target pool is
 * skipped, and one caught halfway is simply copied again, or finished by the
 * MDS's own recovery if its new layout was journaled already.
 *
 * Why not a Journaler, like PurgeQueue?  A purge item stands alone and items
 * finish more or less in order.  Here an item fans out into more items, jobs
 * can be paused and aborted, and each needs counters that outlive the items.
 * With an omap one write does "this directory is done, these are next, here
 * are the new totals", or drops a job's list altogether.  There is also no
 * new journal to create, upgrade or teach the recovery tools about.  Why not
 * scrub itself?  It keeps its stack in memory, and a failover loses it.
 *
 * The list is a stack, so the walk is depth first and holds the subdirectories
 * of one path, not a whole level of the tree.  A write of more than
 * MAX_KEYS_PER_WRITE keys is split, the additions first.
 *
 * mds_migrate_max_files bounds the files in progress, each with up to 16 ops
 * on the OSDs, and the directories being scanned.  A file holds its slot until
 * its old pool is clear, so slow OSDs slow the walk.  Nothing new starts while
 * the target pool is full, and little while the cache or the journal is too
 * big.
 *
 * Files are seen by inode number, so a tree renamed during the walk is still
 * finished, but a file moved or created below a directory already walked is
 * not found.  Hard links are followed, even out of the tree: every link leads
 * to the one inode, which moves once.  Snapshot directories are not walked:
 * the migration of a file takes care of its snapshots or, when it cannot,
 * reports that.  A dirfrag or file on another rank is counted in not_auth and
 * left alone.
 */

#include "MigrateQueue.h"

#include <algorithm>

#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/ceph_fs.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"

#include "CDentry.h"
#include "CDir.h"
#include "CInode.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDSContext.h"
#include "MDSRank.h"
#include "Server.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".migrate_queue "

using ceph::bufferlist;
using ceph::Formatter;

static const size_t MAX_KEYS_PER_WRITE = 1024;
static const uint64_t LOAD_KEYS = 1024;
static const size_t MAX_NOTES = 16;
static const size_t MAX_FINISHED_JOBS = 16;
static const unsigned MAX_BUSY_TRIES = 6;
static const double BUSY_RETRY_INTERVAL = 5.0;
// In a row: most likely it is the pool, and the rest of the tree would fail too.
static const unsigned MAX_FAILURES_IN_A_ROW = 32;

static const char *state_name(uint8_t s)
{
  static const char *names[] = {"running", "paused", "done", "aborted"};
  return s < 4 ? names[s] : "unknown";
}

void MigrateQueue::Note::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(ino, bl);
  encode(path, bl);
  encode(err, bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Note::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(ino, p);
  decode(path, p);
  decode(err, p);
  DECODE_FINISH(p);
}

void MigrateQueue::Note::dump(Formatter *f) const
{
  f->open_object_section("file");
  f->dump_stream("ino") << ino;
  f->dump_string("path", path);
  if (err)
    f->dump_string("error", cpp_strerror(err));
  f->close_section();
}

template<typename T>
static void encode_notes(const std::list<T>& l, bufferlist& bl)
{
  using ceph::encode;
  uint32_t n = l.size();
  encode(n, bl);
  for (const auto& x : l)
    x.encode(bl);
}

template<typename T>
static void decode_notes(std::list<T>& l, bufferlist::const_iterator& p)
{
  using ceph::decode;
  uint32_t n;
  decode(n, p);
  l.clear();
  while (n--) {
    l.emplace_back();
    l.back().decode(p);
  }
}

void MigrateQueue::Progress::add(const Progress& o)
{
  dirs += o.dirs;
  moved += o.moved;
  head_only += o.head_only;
  already += o.already;
  failed += o.failed;
  gone += o.gone;
  other += o.other;
  not_auth += o.not_auth;
  bytes += o.bytes;
  head_only_files.insert(head_only_files.end(), o.head_only_files.begin(),
                         o.head_only_files.end());
  errors.insert(errors.end(), o.errors.begin(), o.errors.end());
  while (head_only_files.size() > MAX_NOTES)
    head_only_files.pop_front();
  while (errors.size() > MAX_NOTES)
    errors.pop_front();
}

void MigrateQueue::Progress::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(dirs, bl);
  encode(moved, bl);
  encode(head_only, bl);
  encode(already, bl);
  encode(failed, bl);
  encode(gone, bl);
  encode(other, bl);
  encode(not_auth, bl);
  encode(bytes, bl);
  encode_notes(head_only_files, bl);
  encode_notes(errors, bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Progress::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(dirs, p);
  decode(moved, p);
  decode(head_only, p);
  decode(already, p);
  decode(failed, p);
  decode(gone, p);
  decode(other, p);
  decode(not_auth, p);
  decode(bytes, p);
  decode_notes(head_only_files, p);
  decode_notes(errors, p);
  DECODE_FINISH(p);
}

void MigrateQueue::Job::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(id, bl);
  encode(root, bl);
  encode(path, bl);
  encode(pool, bl);
  encode(pool_name, bl);
  encode(static_cast<uint8_t>(state), bl);
  encode(reason, bl);
  encode(started, bl);
  encode(finished, bl);
  done.encode(bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Job::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(id, p);
  decode(root, p);
  decode(path, p);
  decode(pool, p);
  decode(pool_name, p);
  uint8_t s;
  decode(s, p);
  state = static_cast<State>(s);
  decode(reason, p);
  decode(started, p);
  decode(finished, p);
  done.decode(p);
  DECODE_FINISH(p);
}

MigrateQueue::MigrateQueue(MDSRank *m) : mds(m)
{
  char n[40];
  snprintf(n, sizeof(n), "mds%d_migratequeue", int(mds->get_nodeid()));
  oid = n;
}

MigrateQueue::~MigrateQueue() = default;

std::string MigrateQueue::job_key(uint64_t id)
{
  char k[32];
  snprintf(k, sizeof(k), "job.%016" PRIx64, id);
  return k;
}

std::string MigrateQueue::dir_key(uint64_t id, inodeno_t ino)
{
  char k[48];
  snprintf(k, sizeof(k), "dir.%016" PRIx64 ".%016" PRIx64, id, uint64_t(ino));
  return k;
}

/* --- persistence ---------------------------------------------------------- */

void MigrateQueue::load()
{
  if (loading)
    return;
  loading = true;
  dout(10) << __func__ << " " << oid << dendl;
  _load_next("");
}

void MigrateQueue::_load_next(const std::string& after)
{
  struct Load {
    std::map<std::string, bufferlist> vals;
    bool more = false;
    int rval = 0;
  };
  auto l = std::make_shared<Load>();
  ObjectOperation op;
  op.omap_get_vals(after, "", LOAD_KEYS, &l->vals, &l->more, &l->rval);
  auto fin = new C_OnFinisher(
    new C_IO_Wrapper(mds, new LambdaContext([this, l](int r) {
      if (r == -ENOENT) {       // nothing was ever queued on this rank
        _load_done();
        return;
      }
      if (r == 0)
        r = l->rval;
      if (r < 0) {
        _load_failed(r);
        return;
      }
      std::string last;
      if (!l->vals.empty())
        last = l->vals.rbegin()->first;
      loaded.merge(l->vals);
      if (l->more)
        _load_next(last);
      else
        _load_done();
    })), mds->finisher);
  mds->objecter->read(object_t(oid), object_locator_t(mds->get_metadata_pool()),
                      op, CEPH_NOSNAP, nullptr, 0, fin);
}

void MigrateQueue::_load_failed(int r)
{
  mds->clog->error() << "cannot read " << oid << ": " << cpp_strerror(r)
                     << ", directory migrations do not run on this rank";
  load_failed = true;
  ready = true;
  MDSContext::vec waiters(ready_waiters.begin(), ready_waiters.end());
  ready_waiters.clear();
  mds->queue_waiters(waiters);
}

void MigrateQueue::_load_done()
{
  std::vector<std::pair<uint64_t,inodeno_t>> dirs;
  try {
    for (auto& [key, val] : loaded) {
      if (key.compare(0, 4, "job.") == 0) {
        Job job;
        auto p = val.cbegin();
        job.decode(p);
        last_id = std::max(last_id, job.id);
        jobs[job.id] = std::move(job);
      } else if (key.compare(0, 4, "dir.") == 0 && key.size() == 37) {
        dirs.emplace_back(std::stoull(key.substr(4, 16), nullptr, 16),
                          inodeno_t(std::stoull(key.substr(21), nullptr, 16)));
      }
    }
  } catch (const std::exception& e) {
    derr << __func__ << " bad entry: " << e.what() << dendl;
    loaded.clear();
    jobs.clear();
    _load_failed(-EIO);
    return;
  }
  loaded.clear();

  for (auto& [id, ino] : dirs) {
    auto p = jobs.find(id);
    if (p == jobs.end() || !p->second.live())
      _rm(dir_key(id, ino));    // a split write was cut short
    else
      p->second.pending.push_back(ino);
  }
  for (auto& [id, job] : jobs) {
    dout(1) << "job " << id << " " << job.path << " to pool " << job.pool_name
            << ": " << state_name(uint8_t(job.state)) << ", "
            << job.pending.size() << " dirs left" << dendl;
    _maybe_finish_job(job);
  }
  _flush(nullptr);

  ready = true;
  MDSContext::vec waiters(ready_waiters.begin(), ready_waiters.end());
  ready_waiters.clear();
  mds->queue_waiters(waiters);
  _kick();
}

void MigrateQueue::wait_for_ready(MDSContext *c)
{
  ceph_assert(!ready);
  ready_waiters.push_back(c);
}

void MigrateQueue::_save_job(const Job& job)
{
  bufferlist bl;
  job.encode(bl);
  _set(job_key(job.id), std::move(bl));
}

void MigrateQueue::_set(const std::string& key, bufferlist&& bl)
{
  to_rm.erase(key);
  to_set[key] = std::move(bl);
}

void MigrateQueue::_rm(const std::string& key)
{
  to_set.erase(key);
  to_rm.insert(key);
}

void MigrateQueue::_flush(Context *fin)
{
  if (fin)
    to_finish.push_back(fin);
  _commit();
}

/*
 * One write at a time; whatever piles up meanwhile goes into the next one.
 * Up to MAX_KEYS_PER_WRITE keys, which is all of it unless a directory has
 * more subdirectories than that, go in one atomic write.  When split, the
 * dir. keys sort first and removals go last, so a cut leaves a directory to
 * walk again, never a subdirectory lost.
 */
void MigrateQueue::_commit()
{
  if (committing)
    return;
  if (to_set.empty() && to_rm.empty()) {
    finish_contexts(g_ceph_context, to_finish);
    to_finish.clear();
    return;
  }

  std::map<std::string, bufferlist> set_keys;
  while (!to_set.empty() && set_keys.size() < MAX_KEYS_PER_WRITE) {
    auto n = to_set.extract(to_set.begin());
    set_keys.insert(std::move(n));
  }
  std::set<std::string> rm_keys;
  if (to_set.empty()) {
    while (!to_rm.empty() &&
           set_keys.size() + rm_keys.size() < MAX_KEYS_PER_WRITE) {
      auto n = to_rm.extract(to_rm.begin());
      rm_keys.insert(std::move(n));
    }
  }

  ObjectOperation op;
  if (!set_keys.empty())
    op.omap_set(set_keys);
  if (!rm_keys.empty())
    op.omap_rm_keys(rm_keys);

  std::vector<Context*> finishers;
  if (to_set.empty() && to_rm.empty())
    finishers.swap(to_finish);

  dout(20) << __func__ << " set " << set_keys.size() << " rm " << rm_keys.size()
           << dendl;
  committing = true;
  auto fin = new C_OnFinisher(
    new C_IO_Wrapper(mds, new LambdaContext(
      [this, finishers = std::move(finishers)](int r) mutable {
        committing = false;
        if (r < 0) {
          derr << "write to " << oid << " failed: " << cpp_strerror(r) << dendl;
          mds->handle_write_error(r);
          return;
        }
        finish_contexts(g_ceph_context, finishers);
        _commit();
      })), mds->finisher);
  mds->objecter->mutate(object_t(oid),
                        object_locator_t(mds->get_metadata_pool()), op,
                        SnapContext(), ceph::real_clock::now(), 0, fin);
}

/* --- jobs ----------------------------------------------------------------- */

void MigrateQueue::enqueue(CInode *in, int64_t pool, Context *fin)
{
  ceph_assert(ready);
  if (load_failed) {
    fin->complete(-EIO);
    return;
  }

  std::string path;
  in->make_path_string(path, true);
  if (path.empty())
    path = "/";
  auto as_prefix = [](std::string p) {
    if (p.back() != '/')
      p += '/';
    return p;
  };
  const std::string prefix = as_prefix(path);
  for (auto& [id, job] : jobs) {
    if (!job.live())
      continue;
    if (job.root == in->ino() && job.pool == pool) {
      _flush(fin);      // maybe a resent request
      return;
    }
    // Nested jobs could pull a file back and forth.
    const std::string other = as_prefix(job.path);
    if (job.root == in->ino() || prefix.starts_with(other) ||
        other.starts_with(prefix)) {
      dout(5) << __func__ << " " << path << " clashes with job " << id
              << " on " << job.path << dendl;
      fin->complete(-EBUSY);
      return;
    }
  }

  Job& job = jobs[++last_id];
  job.id = last_id;
  job.root = in->ino();
  job.path = path;
  job.pool = pool;
  job.pool_name = mds->objecter->with_osdmap([pool](const OSDMap& o) {
    return o.get_pool_name(pool);
  });
  job.started = ceph_clock_now();
  job.pending.push_back(in->ino());
  mds->clog->info() << "migration job " << job.id << " of " << path
                    << " to pool " << job.pool_name << " started";

  _set(dir_key(job.id, in->ino()), {});
  _save_job(job);
  _flush(fin);
  _kick();
}

void MigrateQueue::_maybe_finish_job(Job& job)
{
  if (!job.live() || !job.pending.empty())
    return;
  for (auto& dw : active) {
    if (dw->job == &job)
      return;
  }
  job.state = State::DONE;
  job.finished = ceph_clock_now();
  job.hold.clear();
  const auto& d = job.done;
  mds->clog->info() << "migration job " << job.id << " of " << job.path
                    << " to pool " << job.pool_name << " done: " << d.moved
                    << " files moved (" << d.head_only << " head only), "
                    << d.already << " already there, " << d.failed
                    << " failed, " << d.not_auth << " on other ranks";
  _save_job(job);
}

void MigrateQueue::_abort_job(Job& job, const std::string& reason)
{
  dout(1) << "abort job " << job.id << ": " << reason << dendl;
  job.state = State::ABORTED;
  job.reason = reason;
  job.finished = ceph_clock_now();
  job.hold.clear();
  for (inodeno_t ino : job.pending)
    _rm(dir_key(job.id, ino));
  job.pending.clear();

  std::vector<DirWork*> dirs;
  for (auto& dw : active) {
    if (dw->job == &job)
      dirs.push_back(dw.get());
  }
  for (DirWork *dw : dirs) {
    _rm(dir_key(job.id, dw->ino));
    dw->files.clear();
    dw->next = 0;
    dw->retries.clear();
    dw->subdirs.clear();
    _maybe_finish_dir(dw);
  }
  _save_job(job);
}

// Remember a few finished jobs for the status output, not all of them forever.
void MigrateQueue::_trim_jobs()
{
  size_t finished = 0;
  for (auto& [id, job] : jobs)
    finished += !job.live();
  for (auto p = jobs.begin();
       p != jobs.end() && finished > MAX_FINISHED_JOBS; ) {
    bool busy = p->second.live();
    for (auto& dw : active)
      busy = busy || dw->job == &p->second;
    if (busy) {
      ++p;
      continue;
    }
    _rm(job_key(p->first));
    p = jobs.erase(p);
    --finished;
  }
}

int MigrateQueue::control(Control c, std::optional<uint64_t> id,
                          std::ostream& ss, Context *fin)
{
  if (!ready || load_failed) {
    ss << "the queue is not loaded";
    return -EAGAIN;
  }
  if (id && !jobs.count(*id)) {
    ss << "no job " << *id;
    return -ENOENT;
  }
  unsigned changed = 0;
  for (auto& [i, job] : jobs) {
    if (id && i != *id)
      continue;
    switch (c) {
    case Control::PAUSE:
      if (job.state == State::RUNNING) {
        job.state = State::PAUSED;
        job.reason = "paused by the operator";
        _save_job(job);
        changed++;
      }
      break;
    case Control::RESUME:
      if (job.state == State::PAUSED) {
        job.state = State::RUNNING;
        job.reason.clear();
        job.failures_in_a_row = 0;
        _save_job(job);
        changed++;
      }
      break;
    case Control::ABORT:
      if (job.live()) {
        _abort_job(job, "aborted by the operator");
        changed++;
      }
      break;
    }
  }
  ss << changed << " jobs changed";
  _flush(fin);
  _kick();
  return 0;
}

void MigrateQueue::tick()
{
  _kick(true);
}

void MigrateQueue::dump(Formatter *f) const
{
  f->open_object_section("migrate_queue");
  f->dump_bool("loaded", ready && !load_failed);
  f->dump_unsigned("max_files",
                   g_conf().get_val<uint64_t>("mds_migrate_max_files"));
  f->dump_unsigned("files_in_progress", slots);
  f->dump_string("held_back", hold);

  f->open_array_section("jobs");
  for (const auto& [id, job] : jobs) {
    Progress p = job.done;
    uint64_t dirs_active = 0;
    uint64_t in_flight = 0;
    for (const auto& dw : active) {
      if (dw->job == &job) {
        p.add(dw->prog);
        dirs_active++;
        in_flight += dw->in_flight;
      }
    }
    f->open_object_section("job");
    f->dump_unsigned("id", id);
    f->dump_string("state", state_name(uint8_t(job.state)));
    f->dump_string("reason", job.reason);
    f->dump_string("held_back", job.hold);
    f->dump_string("path", job.path);
    f->dump_stream("root") << job.root;
    f->dump_string("pool", job.pool_name);
    f->dump_stream("started") << job.started;
    if (!job.live())
      f->dump_stream("finished") << job.finished;
    f->dump_unsigned("dirs_done", p.dirs);
    f->dump_unsigned("dirs_active", dirs_active);
    f->dump_unsigned("dirs_queued", job.pending.size());
    f->dump_unsigned("files_in_progress", in_flight);
    f->dump_unsigned("files_moved", p.moved);
    f->dump_unsigned("files_head_only", p.head_only);
    f->dump_unsigned("files_already_there", p.already);
    f->dump_unsigned("files_failed", p.failed);
    f->dump_unsigned("files_gone", p.gone);
    f->dump_unsigned("not_regular", p.other);
    f->dump_unsigned("not_auth", p.not_auth);
    f->dump_unsigned("bytes_moved", p.bytes);
    f->open_array_section("head_only");
    for (const auto& n : p.head_only_files)
      n.dump(f);
    f->close_section();
    f->open_array_section("errors");
    for (const auto& n : p.errors)
      n.dump(f);
    f->close_section();
    f->close_section();
  }
  f->close_section();

  const utime_t now = ceph_clock_now();
  f->open_array_section("in_progress");
  for (const auto& [ino, fif] : files_in_flight) {
    f->open_object_section("file");
    f->dump_stream("ino") << ino;
    f->dump_unsigned("job", fif.job);
    f->dump_float("age", now - fif.started);
    f->close_section();
  }
  f->close_section();
  f->close_section();
}

/* --- the walk ------------------------------------------------------------- */

std::string MigrateQueue::_global_hold() const
{
  if (g_conf().get_val<uint64_t>("mds_migrate_max_files") == 0)
    return "mds_migrate_max_files is 0";
  return "";
}

void MigrateQueue::_check_job(Job& job)
{
  job.hold.clear();
  if (job.state != State::RUNNING)
    return;
  if (!mds->mdsmap->is_data_pool(job.pool)) {
    _abort_job(job, "the pool is no longer a data pool of this file system");
    return;
  }
  bool full = mds->objecter->with_osdmap([&job](const OSDMap& o) {
    const pg_pool_t *p = o.get_pg_pool(job.pool);
    return !p || p->has_flag(pg_pool_t::FLAG_FULL);
  });
  if (full)
    job.hold = "the pool is full";
}

/*
 * A cache well over its limit or a long journal do not stop the walk, they
 * slow it down to what can start in one tick.  Much of what we add to the
 * cache is dirty, and only leaves when the journal moves on, and the journal
 * only lets go of expired segments at the next major one.  If an idle MDS
 * waited for either before writing anything, it would wait forever.
 */
void MigrateQueue::_kick(bool tick)
{
  if (kicking || !ready || load_failed || !mds->is_active())
    return;
  kicking = true;
  hold = _global_hold();
  const bool stopped = !hold.empty();
  const bool slow = mds->mdcache->cache_overfull() ||
    mds->mdlog->get_num_segments() >
      2 * g_conf().get_val<uint64_t>("mds_log_max_segments");
  for (auto& [id, job] : jobs)
    _check_job(job);
  if (!stopped && slow) {
    hold = "the MDS cache or journal is too big, starting a few files per tick";
    if (tick) {
      uint64_t n = g_conf().get_val<uint64_t>("mds_migrate_max_files");
      while (n-- && _start_something())
        ;
    }
  } else if (!stopped) {
    while (_start_something())
      ;
  }
  kicking = false;
  _trim_jobs();
  _flush(nullptr);
}

/*
 * Start one thing: a file of a directory in progress if there is one, then a
 * new directory, which is what keeps it depth first.  Whatever this starts may
 * complete at once and remove a DirWork, so the caller just calls it again.
 */
bool MigrateQueue::_start_something()
{
  const uint64_t max = g_conf().get_val<uint64_t>("mds_migrate_max_files");
  if (slots >= max)
    return false;
  const utime_t now = ceph_clock_now();
  for (auto& dw : active) {
    if (dw->job->state != State::RUNNING || !dw->job->hold.empty())
      continue;
    if (dw->next < dw->files.size()) {
      _start_file(dw.get(), dw->files[dw->next++], 0, false);
      return true;
    }
    if (!dw->retries.empty() && dw->retries.front().when <= now) {
      auto r = dw->retries.front();
      dw->retries.pop_front();
      _start_file(dw.get(), r.ino, r.tries, false);
      return true;
    }
  }
  if (active.size() >= max)
    return false;
  for (auto& [id, job] : jobs) {
    if (job.state == State::RUNNING && job.hold.empty() &&
        !job.pending.empty()) {
      _start_dir(job);
      return true;
    }
  }
  return false;
}

void MigrateQueue::_start_dir(Job& job)
{
  auto dw = std::make_unique<DirWork>();
  dw->job = &job;
  dw->ino = job.pending.back();
  job.pending.pop_back();
  dout(10) << "job " << job.id << " dir " << dw->ino << dendl;
  active.push_back(std::move(dw));
  _scan_dir(active.back().get());
}

MDSContext *MigrateQueue::_scan_again(DirWork *dw)
{
  return new MDSInternalContextWrapper(mds, new LambdaContext([this, dw](int) {
    _scan_dir(dw);
    _kick();
  }));
}

void MigrateQueue::_note(std::list<Note>& l, inodeno_t ino, int err)
{
  Note n;
  n.ino = ino;
  n.err = err;
  if (CInode *in = mds->mdcache->get_inode(ino))
    in->make_path_string(n.path, true);
  l.push_back(std::move(n));
  if (l.size() > MAX_NOTES)
    l.pop_front();
}

void MigrateQueue::_scan_dir(DirWork *dw)
{
  Job& job = *dw->job;
  if (job.state == State::ABORTED) {
    dw->scanned = true;
    _maybe_finish_dir(dw);
    return;
  }

  MDCache *mdcache = mds->mdcache;
  CInode *in = mdcache->get_inode(dw->ino);
  if (!in && !dw->opened) {
    dw->opened = true;
    mdcache->open_ino(dw->ino, mds->get_metadata_pool(), _scan_again(dw));
    return;
  }
  if (!in || !in->is_dir()) {
    dw->prog.gone++;            // removed since it was queued
  } else if (!in->is_auth()) {
    dw->prog.not_auth++;
    _note(dw->prog.errors, dw->ino, -EXDEV);
  } else {
    frag_vec_t frags;
    in->dirfragtree.get_leaves(frags);
    for (const frag_t fg : frags) {
      // A frag split since we saw it is covered; one merged since is seen
      // again, and files and subdirs are just inode numbers to dedup.
      if (std::any_of(dw->frags_done.begin(), dw->frags_done.end(),
                      [fg](frag_t d) { return d.contains(fg); }))
        continue;

      CDir *dir = in->get_or_open_dirfrag(mdcache, fg);
      if (mds->damage_table.is_dirfrag_damaged(dir)) {
        dw->prog.failed++;
        _note(dw->prog.errors, dw->ino, -EIO);
      } else if (!dir->is_auth()) {
        if (dir->is_ambiguous_auth()) {
          dir->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, _scan_again(dw));
          return;
        }
        if (mds->is_cluster_degraded()) {
          mds->wait_for_cluster_recovered(_scan_again(dw));
          return;
        }
        dw->prog.not_auth++;
        _note(dw->prog.errors, dw->ino, -EXDEV);
      } else if (!dir->can_auth_pin()) {
        dir->add_waiter(CDir::WAIT_UNFREEZE, _scan_again(dw));
        return;
      } else if (!dir->is_complete()) {
        dir->fetch(_scan_again(dw));
        return;
      } else {
        _scan_frag(dw, dir);
      }
      dw->frags_done.push_back(fg);
    }
    std::sort(dw->files.begin(), dw->files.end());
    auto end = std::unique(dw->files.begin(), dw->files.end());
    dw->prog.already += dw->files.end() - end;  // two links in one directory
    dw->files.erase(end, dw->files.end());
  }
  dout(10) << "job " << job.id << " dir " << dw->ino << ": "
           << dw->files.size() << " files to move, " << dw->subdirs.size()
           << " subdirs" << dendl;
  dw->scanned = true;
  _maybe_finish_dir(dw);
}

void MigrateQueue::_scan_frag(DirWork *dw, CDir *dir)
{
  const int64_t pool = dw->job->pool;
  for (auto it = dir->begin(); it != dir->end(); ++it) {
    const auto& [key, dn] = *it;
    if (key.snapid != CEPH_NOSNAP)
      continue;
    CDentry::linkage_t *l = dn->get_projected_linkage();
    if (l->is_null())
      continue;
    if (l->is_primary()) {
      CInode *in = l->get_inode();
      if (in->is_dir())
        dw->subdirs.insert(in->ino());
      else if (!in->is_file())
        dw->prog.other++;
      else if (in->get_projected_inode()->layout.pool_id == pool)
        dw->prog.already++;
      else
        dw->files.push_back(in->ino());
    } else {
      const unsigned char t = l->get_remote_d_type();
      if (t == DT_REG || t == DT_UNKNOWN)
        dw->files.push_back(l->get_remote_ino());
      else
        dw->prog.other++;
    }
  }
}

void MigrateQueue::_start_file(DirWork *dw, inodeno_t ino, unsigned tries,
                               bool opened)
{
  dw->in_flight++;
  slots++;

  Job& job = *dw->job;
  MDCache *mdcache = mds->mdcache;
  CInode *in = mdcache->get_inode(ino);
  if (!in && !opened) {
    // Trimmed since the scan, or a hard link's primary elsewhere.
    mdcache->open_ino(ino, -1, new MDSInternalContextWrapper(mds,
      new LambdaContext([this, dw, ino, tries](int) {
        dw->in_flight--;
        slots--;
        _start_file(dw, ino, tries, true);
        _kick();
      })));
    return;
  }

  if (job.state == State::ABORTED) {
    // leave it
  } else if (!in || in->get_projected_inode()->nlink == 0) {
    dw->prog.gone++;
  } else if (!in->is_file()) {
    dw->prog.other++;
  } else if (!in->is_auth()) {
    dw->prog.not_auth++;
    _note(dw->prog.errors, ino, -EXDEV);
  } else if (in->get_projected_inode()->layout.pool_id == job.pool ||
             files_in_flight.count(ino)) {
    dw->prog.already++;         // a second link to it
  } else {
    dout(15) << "job " << job.id << " file " << *in << dendl;
    files_in_flight[ino] = {job.id, ceph_clock_now()};
    auto res = std::make_shared<Server::FileMigrateResult>();
    mds->server->file_migrate(in, job.pool, new LambdaContext(
      [this, dw, ino, tries, res](int r) {
        _file_migrated(dw, ino, tries, r, *res);
      }), res.get());
    return;
  }
  _slot_done(dw);
}

void MigrateQueue::_file_migrated(DirWork *dw, inodeno_t ino, unsigned tries,
                                  int r,
                                  const Server::FileMigrateResult& res)
{
  Job& job = *dw->job;
  dout(15) << "job " << job.id << " file " << ino << ": " << r << " moved "
           << res.moved << " head_only " << res.head_only << dendl;
  if (r == 0 && res.moved) {
    dw->prog.moved++;
    dw->prog.bytes += res.bytes;
    if (res.head_only) {
      dw->prog.head_only++;
      _note(dw->prog.head_only_files, ino, 0);
    }
    job.failures_in_a_row = 0;
  } else if (r == 0) {
    dw->prog.already++;
  } else if (r == -EBUSY && tries < MAX_BUSY_TRIES && job.live()) {
    utime_t when = ceph_clock_now();
    when += BUSY_RETRY_INTERVAL;
    dw->retries.push_back({ino, tries + 1, when});
  } else if (r == -EXDEV) {
    dw->prog.not_auth++;
    _note(dw->prog.errors, ino, r);
  } else {
    dw->prog.failed++;
    _note(dw->prog.errors, ino, r);
    if (r != -EINVAL && r != -EBUSY &&
        ++job.failures_in_a_row == MAX_FAILURES_IN_A_ROW &&
        job.state == State::RUNNING) {
      job.state = State::PAUSED;
      job.reason = std::to_string(MAX_FAILURES_IN_A_ROW) +
                   " files in a row failed, the last with: " + cpp_strerror(r);
      mds->clog->warn() << "migration job " << job.id << " of " << job.path
                        << " paused: " << job.reason;
      _save_job(job);
    }
  }
  _wait_cleanup(dw, ino);
}

/*
 * A file holds its slot until its old pool is clear: that is half the work on
 * the OSDs, and each one still clearing holds a journal segment back.
 */
void MigrateQueue::_wait_cleanup(DirWork *dw, inodeno_t ino)
{
  CInode *in = mds->mdcache->get_inode(ino);
  if (in && in->get_projected_inode()->migrating_from >= 0) {
    in->add_waiter(CInode::WAIT_TRUNC, new MDSInternalContextWrapper(mds,
      new LambdaContext([this, dw, ino](int) {
        _wait_cleanup(dw, ino);
      })));
    return;
  }
  files_in_flight.erase(ino);
  _slot_done(dw);
}

void MigrateQueue::_slot_done(DirWork *dw)
{
  dw->in_flight--;
  slots--;
  _maybe_finish_dir(dw);
  _kick();
}

/*
 * A directory is done: in one write, take it off the list, put its
 * subdirectories on, and add what it did to the job's totals.
 */
void MigrateQueue::_maybe_finish_dir(DirWork *dw)
{
  Job& job = *dw->job;
  if (!dw->scanned || dw->in_flight || dw->next < dw->files.size() ||
      !dw->retries.empty())
    return;

  dw->prog.dirs = 1;
  job.done.add(dw->prog);
  if (job.live()) {
    for (inodeno_t ino : dw->subdirs) {
      _set(dir_key(job.id, ino), {});
      job.pending.push_back(ino);
    }
  }
  _rm(dir_key(job.id, dw->ino));

  active.remove_if([dw](const auto& p) { return p.get() == dw; });
  _maybe_finish_job(job);
  _save_job(job);
  _flush(nullptr);
}
