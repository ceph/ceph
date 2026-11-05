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
 *   job.<id>                 the job: root, target pool, state, counters
 *   dir.<id>.<ino>.<frag>    a directory, or a part of it, with work left in it
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
 * reports that.
 *
 * Ranks.  A walk that meets a dirfrag or a file held by another rank hands it
 * over, and that rank walks on in a sub-job: a job of its own in its own queue
 * object, which knows the origin, the rank and job that started it all.  It
 * follows that a failover of a rank costs what it costs with one rank, whoever
 * started the job.  What had to be new is the hand-over and knowing the end:
 *
 *  - A sub-job talks to its origin only, it does not know the other sub-jobs.
 *    What it finds on a third rank goes through the origin.  The origin alone
 *    decides the job is done, and it knows of all that was ever handed over,
 *    so the job's whole state is in one place.  A tree or a chain of ranks,
 *    each waiting for what it handed on, would make every hand-over cheaper
 *    and everything else harder.  There are as many hand-overs as boundaries
 *    between subtrees, not as many as directories.
 *
 *  - An item goes hand.<id>.<rank>.<seq> in the sender's object in the very
 *    write that takes its parent off the list, and only then on the wire.  The
 *    receiver writes it down as dir.<id>.<ino>.<frag>, together with the
 *    highest number it took, and only then acks; a repeat is recognised by its
 *    number.  The sender drops the key on the ack, and resends until then.
 *    Either side can fail at any point, and either side can lose a message:
 *    the item is in at least one object and not walked twice unless a
 *    directory is, which is safe, see above.  Numbers are never reused after a
 *    crash for something else, since nothing leaves before it is durable.
 *
 *  - A sub-job reports on its own, every tick, until the origin says it can
 *    stop; the reply to a report steers it.  An origin thus needs no retries
 *    of its own, and a rank back from a failure finds out what it missed.
 *    Counters in a report are totals, not increments, so a lost or repeated
 *    one does not matter.
 *
 *  - Pause, resume and abort are given to the origin.  They are done when
 *    durable there, reach the sub-jobs at once if they can, and by the reply
 *    to the next report if not: a rank down at the time applies them when it
 *    is back.  Files in progress on a rank finish.
 *
 *  - A sub-job with nothing left, that had all it handed in acked, says it is
 *    idle, stating the highest item number it took, after that is durable.
 *    The job is done when the origin has nothing left itself and every rank
 *    was idle at the last number given to it.  Nothing can be in transit
 *    then: everything is either acked, so seen by the idle rank, or would make
 *    a rank not idle.  A rank taking a new item after it said so runs again.
 *
 * Whom to hand to is what the cache says.  It can be out of date, or the
 * subtree can move on: the rank that receives something it does not hold hands
 * it over again, and the walk just follows.
 *
 * Why not an MClientRequest, as for a quota realm?  That has no reply to wait
 * for, goes to the inode's auth and not the dirfrag's, and it would be one
 * unrelated job per hand-over, with clashes between them.  Scrub's MMDSScrub
 * has the right shape, but scrub starts over if a rank fails.
 */

#include "MigrateQueue.h"

#include <algorithm>

#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/ceph_fs.h"
#include "messages/MMDSMigrate.h"
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

// Of a message.  An ack lets the next ones go.
static const size_t MAX_ITEMS_PER_MSG = 256;
static const double RESEND_INTERVAL = 5.0;

static const char *state_name(uint8_t s)
{
  static const char *names[] = {"running", "paused", "done", "aborted"};
  return s < 4 ? names[s] : "unknown";
}

void MigrateQueue::Item::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(ino, bl);
  encode(fg, bl);
  encode(file, bl);
  encode(where, bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Item::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(ino, p);
  decode(fg, p);
  decode(file, p);
  decode(where, p);
  DECODE_FINISH(p);
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

void MigrateQueue::Report::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(job, bl);
  encode(state, bl);
  encode(reason, bl);
  encode(hold, bl);
  encode(accepted, bl);
  encode(idle, bl);
  encode(dirs_active, bl);
  encode(dirs_queued, bl);
  encode(files_in_progress, bl);
  encode(control_seq, bl);
  total.encode(bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Report::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(job, p);
  decode(state, p);
  decode(reason, p);
  decode(hold, p);
  decode(accepted, p);
  decode(idle, p);
  decode(dirs_active, p);
  decode(dirs_queued, p);
  decode(files_in_progress, p);
  decode(control_seq, p);
  total.decode(p);
  DECODE_FINISH(p);
}

void MigrateQueue::Link::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(rank, bl);
  encode(sent, bl);
  encode(acked, bl);
  encode(accepted, bl);
  report.encode(bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Link::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(1, p);
  decode(rank, p);
  decode(sent, p);
  decode(acked, p);
  decode(accepted, p);
  report.decode(p);
  DECODE_FINISH(p);
}

void MigrateQueue::Job::encode(bufferlist& bl) const
{
  using ceph::encode;
  ENCODE_START(2, 1, bl);
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
  encode(origin_rank, bl);
  encode(origin_job, bl);
  encode(over, bl);
  encode(control_seq, bl);
  uint32_t n = links.size();
  encode(n, bl);
  for (const auto& [rank, l] : links)
    l.encode(bl);
  ENCODE_FINISH(bl);
}

void MigrateQueue::Job::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  DECODE_START(2, p);
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
  if (struct_v >= 2) {
    decode(origin_rank, p);
    decode(origin_job, p);
    decode(over, p);
    decode(control_seq, p);
    uint32_t n;
    decode(n, p);
    links.clear();
    while (n--) {
      Link l;
      l.decode(p);
      links[l.rank] = std::move(l);
    }
  }
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

std::string MigrateQueue::dir_key(uint64_t id, const Item& item)
{
  char k[56];
  snprintf(k, sizeof(k), "dir.%016" PRIx64 ".%016" PRIx64 ".%08x", id,
           uint64_t(item.ino), unsigned(item.fg));
  return k;
}

std::string MigrateQueue::hand_key(uint64_t id, mds_rank_t rank, uint64_t seq)
{
  char k[56];
  snprintf(k, sizeof(k), "hand.%016" PRIx64 ".%04x.%016" PRIx64, id,
           unsigned(rank), seq);
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
  std::vector<std::pair<uint64_t,Item>> dirs;
  struct Hand {
    uint64_t id;
    mds_rank_t rank;
    uint64_t seq;
    Item item;
  };
  std::vector<Hand> hands;
  try {
    for (auto& [key, val] : loaded) {
      if (key.compare(0, 4, "job.") == 0) {
        Job job;
        auto p = val.cbegin();
        job.decode(p);
        last_id = std::max(last_id, job.id);
        jobs[job.id] = std::move(job);
      } else if (key.compare(0, 4, "dir.") == 0 && key.size() == 46) {
        Item item;
        item.ino = inodeno_t(std::stoull(key.substr(21, 16), nullptr, 16));
        item.fg = frag_t(std::stoul(key.substr(38), nullptr, 16));
        item.file = val.length() > 0;
        dirs.emplace_back(std::stoull(key.substr(4, 16), nullptr, 16), item);
      } else if (key.compare(0, 5, "hand.") == 0 && key.size() == 43) {
        Hand h;
        h.id = std::stoull(key.substr(5, 16), nullptr, 16);
        h.rank = std::stoul(key.substr(22, 4), nullptr, 16);
        h.seq = std::stoull(key.substr(27), nullptr, 16);
        auto p = val.cbegin();
        h.item.decode(p);
        hands.push_back(std::move(h));
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

  for (auto& [id, item] : dirs) {
    auto p = jobs.find(id);
    if (p == jobs.end() || !p->second.live())
      _rm(dir_key(id, item));   // a split write was cut short
    else
      p->second.pending.push_back(item);
  }
  for (auto& h : hands) {
    auto p = jobs.find(h.id);
    Link *l = nullptr;
    if (p != jobs.end() && p->second.live()) {
      auto q = p->second.links.find(h.rank);
      if (q != p->second.links.end() && h.seq > q->second.acked)
        l = &q->second;
    }
    if (l) {
      l->out[h.seq] = h.item;
      l->sent = std::max(l->sent, h.seq);
    } else {
      _rm(hand_key(h.id, h.rank, h.seq));
    }
  }
  for (auto& [id, job] : jobs) {
    dout(1) << "job " << id << " " << job.path << " to pool " << job.pool_name
            << ": " << state_name(uint8_t(job.state)) << ", "
            << job.pending.size() << " dirs left" << dendl;
    for (auto& [rank, l] : job.links)
      l.durable = l.sent;       // it was read back
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

void MigrateQueue::_queue_item(Job& job, const Item& item)
{
  bufferlist bl;
  if (item.file)
    bl.append('f');
  _set(dir_key(job.id, item), std::move(bl));
  job.pending.push_back(item);
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
 * dir. and hand. keys sort before job. keys and removals go last, so a cut
 * leaves a directory to walk again or a hand-over to take again, never a
 * subdirectory lost.
 */
void MigrateQueue::_commit()
{
  if (committing)
    return;
  if (to_set.empty() && to_rm.empty()) {
    finish_contexts(g_ceph_context, to_finish);
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
    if (!job.open())
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
  mds->clog->info() << "migration job " << job.id << " of " << path
                    << " to pool " << job.pool_name << " started";

  Item root;
  root.ino = in->ino();
  _queue_item(job, root);
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
  for (const auto& [rank, l] : job.links) {
    if (!_settled(job, l))
      return;
  }
  job.state = State::DONE;
  job.finished = ceph_clock_now();
  job.hold.clear();
  _save_job(job);
  if (job.is_sub()) {
    // What it found elsewhere is the origin's now, and this will soon be too.
    _flush(new LambdaContext([this, id = job.id](int) {
      auto p = jobs.find(id);
      if (p != jobs.end() && p->second.state == State::DONE)
        _report(p->second);
    }));
    return;
  }
  const Progress d = _total(job);
  mds->clog->info() << "migration job " << job.id << " of " << job.path
                    << " to pool " << job.pool_name << " done: " << d.moved
                    << " files moved (" << d.head_only << " head only), "
                    << d.already << " already there, " << d.failed
                    << " failed" << (d.failed ? ": it is not complete" : "");
  _tell_ranks(job);
}

void MigrateQueue::_abort_job(Job& job, const std::string& reason)
{
  dout(1) << "abort job " << job.id << ": " << reason << dendl;
  job.state = State::ABORTED;
  job.reason = reason;
  job.finished = ceph_clock_now();
  job.hold.clear();
  for (const Item& item : job.pending)
    _rm(dir_key(job.id, item));
  job.pending.clear();
  for (auto& [rank, l] : job.links) {
    for (auto& [seq, item] : l.out) {
      _rm(hand_key(job.id, rank, seq));
      job.done.not_auth++;
    }
    l.out.clear();
  }

  std::vector<DirWork*> dirs;
  for (auto& dw : active) {
    if (dw->job == &job)
      dirs.push_back(dw.get());
  }
  for (DirWork *dw : dirs) {
    _rm(dir_key(job.id, dw->item));
    dw->files.clear();
    dw->next = 0;
    dw->retries.clear();
    dw->subdirs.clear();
    dw->remote.clear();
    _maybe_finish_dir(dw);
  }
  _save_job(job);
  _tell_ranks(job);
}

// Remember a few finished jobs for the status output, not all of them forever.
void MigrateQueue::_trim_jobs()
{
  size_t finished = 0;
  for (auto& [id, job] : jobs)
    finished += !job.live();
  for (auto p = jobs.begin();
       p != jobs.end() && finished > MAX_FINISHED_JOBS; ) {
    bool busy = p->second.live() || (p->second.is_sub() && !p->second.over);
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
  if (id && jobs[*id].is_sub()) {
    const Job& job = jobs[*id];
    ss << "job " << *id << " is a part of job " << job.origin_job
       << " of mds." << job.origin_rank << ", steer that one";
    return -EINVAL;
  }
  unsigned changed = 0;
  unsigned ranks = 0;
  unsigned parts = 0;
  for (auto& [i, job] : jobs) {
    if (id && i != *id)
      continue;
    if (job.is_sub()) {
      parts += job.live();
      continue;
    }
    bool any = false;
    bool rank_paused = false;
    for (const auto& [rank, l] : job.links)
      rank_paused = rank_paused || l.report.state == uint8_t(State::PAUSED);
    switch (c) {
    case Control::PAUSE:
      if (job.state == State::RUNNING) {
        job.state = State::PAUSED;
        job.reason = "paused by the operator";
        any = true;
      }
      break;
    case Control::RESUME:
      if (job.state == State::PAUSED) {
        job.state = State::RUNNING;
        job.reason.clear();
        job.failures_in_a_row = 0;
        any = true;
      } else if (job.state == State::RUNNING && rank_paused) {
        any = true;             // a rank paused by itself
      }
      break;
    case Control::ABORT:
      if (job.live()) {
        _abort_job(job, "aborted by the operator");
        changed++;
        ranks += job.links.size();
      }
      break;
    }
    if (any) {
      job.control_seq++;
      _save_job(job);
      _tell_ranks(job);
      changed++;
      ranks += job.links.size();
    }
  }
  ss << changed << " jobs changed";
  if (ranks)
    ss << ", " << ranks << " sub-jobs told; one on a rank that is down follows "
       << "when the rank is back, files in progress finish";
  if (parts)
    ss << ", left alone " << parts << " parts of jobs of other ranks: steer "
       << "those on their origin rank";
  _flush(fin);
  _kick();
  return 0;
}

void MigrateQueue::tick()
{
  if (!ready || load_failed)
    return;
  _tick_links();
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
    uint64_t dirs_active = 0;
    uint64_t in_flight = 0;
    const Progress p = _total(job, &dirs_active, &in_flight);
    for (const auto& [rank, l] : job.links) {
      dirs_active += job.is_sub() ? 0 : l.report.dirs_active;
      in_flight += job.is_sub() ? 0 : l.report.files_in_progress;
    }
    uint64_t dirs_queued = job.pending.size();
    uint64_t handed = 0;
    uint64_t unacked = 0;
    bool settled = true;
    for (const auto& [rank, l] : job.links) {
      dirs_queued += job.is_sub() ? 0 : l.report.dirs_queued;
      handed += l.sent;
      unacked += l.out.size();
      settled = settled && _settled(job, l);
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
    if (job.is_sub()) {
      // Numbers of this rank alone.
      f->dump_int("origin_rank", job.origin_rank);
      f->dump_unsigned("origin_job", job.origin_job);
      f->dump_string("see", "the whole job: migrate status on mds rank " +
                     std::to_string(job.origin_rank));
    }
    // Everything went, or was at least tried: failures are listed below.
    f->dump_bool("complete", job.state == State::DONE && !job.is_sub() &&
                 p.failed == 0 && p.not_auth == 0);
    f->dump_unsigned("dirs_done", p.dirs);
    f->dump_unsigned("dirs_active", dirs_active);
    f->dump_unsigned("dirs_queued", dirs_queued);
    f->dump_unsigned("files_in_progress", in_flight);
    f->dump_unsigned("files_moved", p.moved);
    f->dump_unsigned("files_head_only", p.head_only);
    f->dump_unsigned("files_already_there", p.already);
    f->dump_unsigned("files_failed", p.failed);
    f->dump_unsigned("files_gone", p.gone);
    f->dump_unsigned("not_regular", p.other);
    f->dump_unsigned("not_auth", p.not_auth);
    f->dump_unsigned("bytes_moved", p.bytes);
    f->dump_unsigned("items_handed_over", handed);
    f->dump_unsigned("items_not_taken_yet", unacked);
    f->dump_bool("other_ranks_finished", settled);
    f->open_array_section("ranks");
    for (const auto& [rank, l] : job.links) {
      const Report& r = l.report;
      f->open_object_section("rank");
      f->dump_int("rank", rank);
      f->dump_string("rank_state", !mds->mdsmap->is_in(rank) ? "stopped" :
                     ceph_mds_state_name(mds->mdsmap->get_state(rank)));
      f->dump_unsigned("items_sent", l.sent);
      f->dump_unsigned("items_acked", l.acked);
      f->dump_unsigned("items_taken", l.accepted);
      f->dump_string("refused", l.refused);
      if (!job.is_sub()) {
        f->dump_unsigned("job", r.job);
        f->dump_string("state", r.job ? state_name(r.state) : "no report yet");
        f->dump_string("reason", r.reason);
        f->dump_string("held_back", r.hold);
        f->dump_bool("finished", _settled(job, l));
        f->dump_bool("control_applied", r.control_seq == job.control_seq);
        f->dump_unsigned("dirs_done", r.total.dirs);
        f->dump_unsigned("dirs_active", r.dirs_active);
        f->dump_unsigned("dirs_queued", r.dirs_queued);
        f->dump_unsigned("files_in_progress", r.files_in_progress);
        f->dump_unsigned("files_moved", r.total.moved);
        f->dump_unsigned("files_failed", r.total.failed);
        f->dump_unsigned("bytes_moved", r.total.bytes);
      }
      f->close_section();
    }
    f->close_section();
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
    if (job.is_sub())
      job.hold = "the pool is no longer a data pool, up to the origin";
    else
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
  dw->item = job.pending.back();
  job.pending.pop_back();
  dout(10) << "job " << job.id << " dir " << dw->item.ino << " "
           << dw->item.fg << dendl;
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
  const inodeno_t ino = dw->item.ino;
  if (job.state == State::ABORTED) {
    dw->scanned = true;
    _maybe_finish_dir(dw);
    return;
  }
  if (dw->item.file) {
    dw->files.push_back(ino);
    dw->scanned = true;
    _maybe_finish_dir(dw);
    return;
  }

  MDCache *mdcache = mds->mdcache;
  CInode *in = mdcache->get_inode(ino);
  if (!in && !dw->opened) {
    dw->opened = true;
    mdcache->open_ino(ino, mds->get_metadata_pool(), _scan_again(dw));
    return;
  }
  if (!in || !in->is_dir()) {
    dw->prog.gone++;            // removed since it was queued
  } else {
    // The inode may well be on another rank: what counts is the dirfrags.
    frag_vec_t frags;
    in->dirfragtree.get_leaves(frags);
    for (const frag_t fg : frags) {
      if (!dw->item.fg.contains(fg) && !fg.contains(dw->item.fg))
        continue;
      // A frag split since we saw it is covered; one merged since is seen
      // again, and files and subdirs are just inode numbers to dedup.
      if (std::any_of(dw->frags_done.begin(), dw->frags_done.end(),
                      [fg](frag_t d) { return d.contains(fg); }))
        continue;

      Item part;
      part.ino = ino;
      part.fg = fg;
      CDir *dir = in->get_dirfrag(fg);
      if (!dir && !in->is_auth()) {
        // Not known here, so not held here; the inode's auth does know.
        part.where = in->authority().first;
        dw->remote.push_back(part);
        dw->frags_done.push_back(fg);
        continue;
      }
      if (!dir)
        dir = in->get_or_open_dirfrag(mdcache, fg);
      if (mds->damage_table.is_dirfrag_damaged(dir)) {
        dw->prog.failed++;
        _note(dw->prog.errors, ino, -EIO);
      } else if (!dir->is_auth()) {
        if (dir->is_ambiguous_auth()) {
          dir->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, _scan_again(dw));
          return;
        }
        part.where = dir->authority().first;
        dw->remote.push_back(part);
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
  dout(10) << "job " << job.id << " dir " << ino << ": "
           << dw->files.size() << " files to move, " << dw->subdirs.size()
           << " subdirs, " << dw->remote.size() << " to hand over" << dendl;
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
    // A hard link to a file held elsewhere, or exported since the scan.
    Item item;
    item.ino = ino;
    item.file = true;
    item.where = in->authority().first;
    dw->remote.push_back(item);
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
    // Exported while it waited for a lock.
    Item item;
    item.ino = ino;
    item.file = true;
    if (CInode *in = mds->mdcache->get_inode(ino))
      item.where = in->authority().first;
    dw->remote.push_back(item);
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

  dw->prog.dirs = dw->item.file ? 0 : 1;
  job.done.add(dw->prog);
  _rm(dir_key(job.id, dw->item));
  if (job.live()) {
    for (inodeno_t ino : dw->subdirs) {
      Item item;
      item.ino = ino;
      _queue_item(job, item);
    }
    for (const Item& item : dw->remote)
      _route(job, item);
  }

  active.remove_if([dw](const auto& p) { return p.get() == dw; });
  _maybe_finish_job(job);
  _save_job(job);
  _flush(_on_durable(job));
}

/* --- other ranks ---------------------------------------------------------- */

MigrateQueue::Job *MigrateQueue::_find_sub(mds_rank_t origin, uint64_t id)
{
  for (auto& [i, job] : jobs) {
    if (job.origin_rank == origin && job.origin_job == id)
      return &job;
  }
  return nullptr;
}

MigrateQueue::Link& MigrateQueue::_link(Job& job, mds_rank_t rank)
{
  Link& l = job.links[rank];
  l.rank = rank;
  return l;
}

// Is all that went over this link finished?  Only the origin cares about more
// than that the items arrived.
bool MigrateQueue::_settled(const Job& job, const Link& l) const
{
  if (!l.out.empty())
    return false;
  return job.is_sub() || l.sent == 0 ||
         (l.report.idle && l.report.accepted >= l.sent);
}

/*
 * A sub-job gives everything to the origin, and the origin gives it to the
 * rank said to hold it.  If nobody knows, or that rank has left, the origin
 * walks it itself, and sees whose it is by then.
 */
void MigrateQueue::_route(Job& job, const Item& item)
{
  const mds_rank_t self = mds->get_nodeid();
  if (job.is_sub()) {
    _hand_over(job, job.origin_rank, item);
  } else if (item.where < 0 || item.where == self ||
             !mds->mdsmap->is_in(item.where)) {
    _queue_item(job, item);
  } else {
    _hand_over(job, item.where, item);
  }
}

void MigrateQueue::_hand_over(Job& job, mds_rank_t to, const Item& item)
{
  Link& l = _link(job, to);
  const uint64_t seq = ++l.sent;
  dout(10) << "job " << job.id << " hands " << item.ino << " " << item.fg
           << (item.file ? " file" : "") << " to mds." << to << " as " << seq
           << dendl;
  l.out[seq] = item;
  bufferlist bl;
  item.encode(bl);
  _set(hand_key(job.id, to, seq), std::move(bl));
}

// To flush with: lets the items handed over so far go out.
Context *MigrateQueue::_on_durable(const Job& job)
{
  std::map<mds_rank_t, uint64_t> upto;
  for (const auto& [rank, l] : job.links) {
    if (l.durable < l.sent)
      upto[rank] = l.sent;
  }
  if (upto.empty())
    return nullptr;
  return new LambdaContext([this, id = job.id, upto = std::move(upto)](int) {
    ceph_assert(g_conf().get_val<int64_t>("mds_kill_file_migrate_at") != 6);
    auto p = jobs.find(id);
    if (p == jobs.end())
      return;
    for (const auto& [rank, seq] : upto) {
      Link& l = _link(p->second, rank);
      l.durable = std::max(l.durable, seq);
      _send_items(p->second, l, false);
    }
  });
}

void MigrateQueue::_send_msg(int op, mds_rank_t to, mds_rank_t origin,
                             uint64_t id, bufferlist&& bl)
{
  auto m = make_message<MMDSMigrate>(op, origin, id, std::move(bl));
  mds->send_message_mds(m, to);
}

/*
 * One message at a time on a link.  An origin holds back while its job is
 * paused; a sub-job always hands in what it found, it has no use for it.
 */
void MigrateQueue::_send_items(Job& job, Link& l, bool resend)
{
  if (!job.is_sub() && job.state != State::RUNNING)
    return;
  if (l.out.empty() || (!resend && l.on_wire > l.acked))
    return;

  using ceph::encode;
  std::vector<std::pair<uint64_t, const Item*>> items;
  for (auto p = l.out.begin();
       p != l.out.end() && p->first <= l.durable &&
         items.size() < MAX_ITEMS_PER_MSG; ++p)
    items.emplace_back(p->first, &p->second);
  if (items.empty())
    return;

  bufferlist bl;
  encode(job.root, bl);
  encode(job.path, bl);
  encode(job.pool, bl);
  encode(job.pool_name, bl);
  encode(job.control_seq, bl);
  encode(uint32_t(items.size()), bl);
  for (const auto& [seq, item] : items) {
    encode(seq, bl);
    item->encode(bl);
  }
  l.on_wire = items.back().first;
  l.last_sent = ceph_clock_now();
  dout(10) << "job " << job.id << " sends " << items.size() << " items up to "
           << l.on_wire << " to mds." << l.rank << dendl;
  const mds_rank_t origin = job.is_sub() ? job.origin_rank : mds->get_nodeid();
  _send_msg(MMDSMigrate::OP_ITEMS, l.rank, origin,
            job.is_sub() ? job.origin_job : job.id, std::move(bl));
}

void MigrateQueue::_send_ack(mds_rank_t to, mds_rank_t origin, uint64_t id,
                             uint64_t accepted, int err,
                             const std::string& reason)
{
  using ceph::encode;
  bufferlist bl;
  encode(accepted, bl);
  encode(err, bl);
  encode(reason, bl);
  _send_msg(MMDSMigrate::OP_ACK, to, origin, id, std::move(bl));
}

MigrateQueue::Progress MigrateQueue::_total(const Job& job,
                                            uint64_t *dirs_active,
                                            uint64_t *in_flight) const
{
  Progress p = job.done;
  for (const auto& dw : active) {
    if (dw->job == &job) {
      p.add(dw->prog);
      if (dirs_active)
        (*dirs_active)++;
      if (in_flight)
        *in_flight += dw->in_flight;
    }
  }
  if (!job.is_sub()) {
    for (const auto& [rank, l] : job.links)
      p.add(l.report.total);
  }
  return p;
}

void MigrateQueue::_report(Job& job)
{
  Report r;
  r.job = job.id;
  r.state = uint8_t(job.state);
  r.reason = job.reason;
  r.hold = job.hold;
  r.accepted = _link(job, job.origin_rank).accepted;
  r.idle = job.state == State::DONE;
  r.control_seq = job.control_seq;
  r.dirs_queued = job.pending.size();
  r.total = _total(job, &r.dirs_active, &r.files_in_progress);
  bufferlist bl;
  r.encode(bl);
  _send_msg(MMDSMigrate::OP_REPORT, job.origin_rank, job.origin_rank,
            job.origin_job, std::move(bl));
}

/*
 * What the reports get in reply, and what goes out at once when the job
 * changes.  The caller sees to it that what it says is durable by now.  seen:
 * the origin has written down that the rank was idle at that number.
 */
void MigrateQueue::_tell_rank(uint64_t id, mds_rank_t rank, uint64_t seen)
{
  using ceph::encode;
  auto p = jobs.find(id);
  bool over = p == jobs.end() || !p->second.live();
  bool paused = !over && p->second.state == State::PAUSED;
  uint64_t control_seq = over ? 0 : p->second.control_seq;
  std::string reason = over ? "" : p->second.reason;
  bufferlist bl;
  encode(over, bl);
  encode(seen, bl);
  encode(paused, bl);
  encode(control_seq, bl);
  encode(reason, bl);
  _send_msg(MMDSMigrate::OP_CONTROL, rank, mds->get_nodeid(), id,
            std::move(bl));
}

void MigrateQueue::_tell_ranks(const Job& job)
{
  if (job.is_sub() || job.links.empty())
    return;
  std::vector<mds_rank_t> ranks;
  for (const auto& [rank, l] : job.links)
    ranks.push_back(rank);
  _flush(new LambdaContext([this, id = job.id, ranks = std::move(ranks)](int) {
    for (mds_rank_t rank : ranks)
      _tell_rank(id, rank, 0);
  }));
}

void MigrateQueue::_tick_links()
{
  const utime_t now = ceph_clock_now();
  for (auto& [id, job] : jobs) {
    if (!job.is_sub() && job.live()) {
      _check_gone_ranks(job);
      _maybe_finish_job(job);
    }
    for (auto& [rank, l] : job.links) {
      if (now - l.last_sent >= RESEND_INTERVAL)
        _send_items(job, l, true);
    }
    if (job.is_sub() && !job.over)
      _report(job);
  }
}

/*
 * A rank down:stopped is not coming back soon, and whatever it did not take
 * goes elsewhere.  What it did take it finished or gave back: it does not stop
 * before that.
 */
void MigrateQueue::_check_gone_ranks(Job& job)
{
  for (auto& [rank, l] : job.links) {
    if (mds->mdsmap->is_in(rank))
      continue;
    const uint64_t taken = l.report.accepted;
    for (auto& [seq, item] : l.out) {
      _rm(hand_key(job.id, rank, seq));
      if (seq > taken) {
        Item i = item;
        i.where = MDS_RANK_NONE;
        _queue_item(job, i);
      }
    }
    if (!l.out.empty() || l.sent != taken) {
      dout(1) << "job " << job.id << ": mds." << rank << " has stopped, took "
              << taken << " of " << l.sent << dendl;
      l.out.clear();
      l.sent = l.acked = l.on_wire = l.durable = taken;
      l.refused.clear();
      _save_job(job);
    }
  }
}

// Two jobs on overlapping trees are a problem only if they pull apart.
std::string MigrateQueue::_clash(const std::string& path, int64_t pool) const
{
  auto as_prefix = [](std::string p) {
    if (p.back() != '/')
      p += '/';
    return p;
  };
  const std::string prefix = as_prefix(path);
  for (const auto& [id, job] : jobs) {
    if (!job.open() || job.pool == pool)
      continue;
    const std::string other = as_prefix(job.path);
    if (prefix.starts_with(other) || other.starts_with(prefix))
      return "job " + std::to_string(id) + " of " + job.path + " to pool " +
             job.pool_name + " on mds." + std::to_string(mds->get_nodeid());
  }
  return "";
}

void MigrateQueue::dispatch(const cref_t<Message>& m)
{
  // Everything is sent again if it had no effect, by whoever needs it.
  if (!ready || load_failed)
    return;
  const double drop = g_conf().get_val<double>("mds_migrate_inject_drop");
  if (drop > 0 && drop * RAND_MAX > rand()) {
    dout(10) << "dropping " << *m << dendl;
    return;
  }
  auto msg = ref_cast<MMDSMigrate>(m);
  const mds_rank_t from = mds_rank_t(m->get_source().num());
  try {
    switch (msg->op) {
    case MMDSMigrate::OP_ITEMS:
      _handle_items(from, msg);
      break;
    case MMDSMigrate::OP_ACK:
      _handle_ack(from, msg);
      break;
    case MMDSMigrate::OP_REPORT:
      _handle_report(from, msg);
      break;
    case MMDSMigrate::OP_CONTROL:
      _handle_control(msg);
      break;
    default:
      derr << "bad op in " << *m << dendl;
    }
  } catch (const ceph::buffer::error& e) {
    derr << "bad " << *m << " from mds." << from << ": " << e.what() << dendl;
  }
  _kick();
}

void MigrateQueue::_handle_items(mds_rank_t from,
                                 const cref_t<MMDSMigrate>& m)
{
  using ceph::decode;
  inodeno_t root;
  std::string path, pool_name;
  int64_t pool;
  uint64_t control_seq;
  uint32_t n;
  auto p = m->data.cbegin();
  decode(root, p);
  decode(path, p);
  decode(pool, p);
  decode(pool_name, p);
  decode(control_seq, p);
  decode(n, p);
  std::vector<std::pair<uint64_t, Item>> items(n);
  for (auto& [seq, item] : items) {
    decode(seq, p);
    item.decode(p);
  }
  const uint64_t last = items.empty() ? 0 : items.back().first;

  const bool origin = m->origin == mds->get_nodeid();
  Job *job = nullptr;
  if (origin) {
    auto q = jobs.find(m->job);
    if (q == jobs.end() || !q->second.live()) {
      _send_ack(from, m->origin, m->job, last, 0, "");  // it is over
      return;
    }
    job = &q->second;
  } else {
    job = _find_sub(m->origin, m->job);
    if (job && (job->over || job->state == State::ABORTED)) {
      _send_ack(from, m->origin, m->job, last, 0, "");
      return;
    }
    if (!job) {
      const std::string clash = _clash(path, pool);
      if (!clash.empty()) {
        dout(5) << "refusing " << *m << ": " << clash << dendl;
        _send_ack(from, m->origin, m->job, 0, -EBUSY, clash);
        return;
      }
      job = &jobs[++last_id];
      job->id = last_id;
      job->root = root;
      job->path = path;
      job->pool = pool;
      job->pool_name = pool_name;
      job->started = ceph_clock_now();
      job->origin_rank = m->origin;
      job->origin_job = m->job;
      job->control_seq = control_seq;
      dout(1) << "job " << job->id << ": part of job " << m->job << " of mds."
              << m->origin << ", " << path << " to pool " << pool_name << dendl;
    }
  }

  Link& l = _link(*job, from);
  bool any = false;
  for (const auto& [seq, item] : items) {
    if (seq <= l.accepted)
      continue;                 // a repeat: its ack got lost
    any = true;
    if (origin)
      _route(*job, item);
    else
      _queue_item(*job, item);
    l.accepted = seq;
  }
  if (any && job->state == State::DONE) {
    job->state = State::RUNNING;  // more came in after it reported
    job->finished = utime_t();
  }
  _save_job(*job);
  Context *handed = _on_durable(*job);
  _flush(new LambdaContext(
    [this, handed, from, o = m->origin, id = m->job, accepted = l.accepted](int) {
      ceph_assert(g_conf().get_val<int64_t>("mds_kill_file_migrate_at") != 7);
      _send_ack(from, o, id, accepted, 0, "");
      if (handed)
        handed->complete(0);
    }));
}

void MigrateQueue::_handle_ack(mds_rank_t from, const cref_t<MMDSMigrate>& m)
{
  using ceph::decode;
  uint64_t accepted;
  int32_t err;
  std::string reason;
  auto p = m->data.cbegin();
  decode(accepted, p);
  decode(err, p);
  decode(reason, p);

  Job *job = nullptr;
  if (m->origin == mds->get_nodeid()) {
    auto q = jobs.find(m->job);
    if (q != jobs.end() && !q->second.is_sub())
      job = &q->second;
  } else {
    job = _find_sub(m->origin, m->job);
  }
  if (!job)
    return;
  Link& l = _link(*job, from);
  l.refused = reason;
  if (!err)
    l.refused.clear();
  if (accepted > l.acked) {
    l.acked = accepted;
    for (auto q = l.out.begin(); q != l.out.end() && q->first <= accepted; ) {
      _rm(hand_key(job->id, from, q->first));
      q = l.out.erase(q);
    }
    _save_job(*job);
    _flush(nullptr);
  }
  if (err) {
    dout(5) << "job " << job->id << ": mds." << from << " refuses: " << reason
            << dendl;
    return;                     // the tick tries again
  }
  l.on_wire = std::max(l.on_wire, accepted);
  _send_items(*job, l, false);
  _maybe_finish_job(*job);
}

void MigrateQueue::_handle_report(mds_rank_t from,
                                  const cref_t<MMDSMigrate>& m)
{
  Report r;
  auto p = m->data.cbegin();
  r.decode(p);

  auto q = jobs.find(m->job);
  if (q == jobs.end() || q->second.is_sub()) {
    _tell_rank(m->job, from, 0);
    return;
  }
  Job& job = q->second;
  Link& l = _link(job, from);
  // Worth keeping across a failover only when it says the rank is finished.
  const bool idler = r.idle && (!l.report.idle ||
                                r.accepted != l.report.accepted);
  l.report = std::move(r);
  if (idler)
    _save_job(job);
  _maybe_finish_job(job);
  const uint64_t seen = l.report.idle ? l.report.accepted : 0;
  _flush(new LambdaContext([this, id = m->job, from, seen](int) {
    _tell_rank(id, from, seen);
  }));
}

void MigrateQueue::_handle_control(const cref_t<MMDSMigrate>& m)
{
  using ceph::decode;
  bool over;
  uint64_t seen;
  bool paused;
  uint64_t control_seq;
  std::string reason;
  auto p = m->data.cbegin();
  decode(over, p);
  decode(seen, p);
  decode(paused, p);
  decode(control_seq, p);
  decode(reason, p);

  Job *job = _find_sub(m->origin, m->job);
  if (!job || job->over)
    return;
  if (over) {
    if (job->live())
      _abort_job(*job, "the job is over on its origin");
    job->over = true;
    _save_job(*job);
    _report(*job);              // for the record, in case it was an abort
  } else if (control_seq > job->control_seq) {
    job->control_seq = control_seq;
    if (paused && job->state == State::RUNNING) {
      job->state = State::PAUSED;
      job->reason = reason;
    } else if (!paused && job->state == State::PAUSED) {
      job->state = State::RUNNING;
      job->reason.clear();
      job->failures_in_a_row = 0;
    }
    _save_job(*job);
    _report(*job);
  }
  _flush(nullptr);
}
