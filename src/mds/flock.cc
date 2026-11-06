// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mds/flock.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "include/ceph_fs_encoder.h"
#include "include/container_ios.h"
#include "mdstypes.h"

#include <boost/container/small_vector.hpp>

#include <iostream>
#include <iterator>
#include <optional>
#include <algorithm>
#include <functional>

#define dout_subsys ceph_subsys_mds

using std::pair;
using std::multimap;

static multimap<ceph_filelock, ceph_lock_state_t*> global_waiting_locks;

struct ceph_lock_state_t::lock_overlaps final {
  // File-lock operations normally overlap only a few ranges. Keep the common
  // case allocation-free while allowing unusually fragmented ranges to grow.
  static constexpr auto inline_capacity = 4;
  using batch = boost::container::small_vector<lock_iterator, inline_capacity>;

  batch other;
  batch owned;
  batch neighbors;

  auto find_exclusive() const
    -> std::optional<std::reference_wrapper<const ceph_filelock>>
  {
    const auto match = std::ranges::find_if(other, [](const auto current) {
      return CEPH_LOCK_EXCL == current->second.type;
    });

    if (std::end(other) == match) {
      return std::nullopt;
    }

    return std::cref((*match)->second);
  }
};

std::ostream& operator<<(std::ostream& out, const ceph_filelock& l) {
  out << "start: " << l.start << ", length: " << l.length
      << ", client: " << l.client << ", owner: " << l.owner
      << ", pid: " << l.pid << ", type: " << (int)l.type
      << std::endl;
  return out;
}

static void remove_global_waiting(ceph_filelock &fl, ceph_lock_state_t *lock_state)
{
  for (auto p = global_waiting_locks.find(fl);
       p != global_waiting_locks.end(); ) {
    if (p->first != fl)
      break;
    if (p->second == lock_state) {
      global_waiting_locks.erase(p);
      break;
    }
    ++p;
  }
}

ceph_lock_state_t::~ceph_lock_state_t()
{
  if (type == CEPH_LOCK_FCNTL) {
    for (auto p = waiting_locks.begin(); p != waiting_locks.end(); ++p) {
      remove_global_waiting(p->second, this);
    }
  }
}

void ceph_lock_state_t::encode(ceph::bufferlist& bl) const {
  using ceph::encode;
  encode(held_locks, bl);
  encode(client_held_lock_counts, bl);
}

void ceph_lock_state_t::decode(ceph::bufferlist::const_iterator& bl) {
  using ceph::decode;
  decode(held_locks, bl);
  decode(client_held_lock_counts, bl);
}

void ceph_lock_state_t::dump(ceph::Formatter *f) const {
  // do not dump fields which are not persisted:
  // - type: set in constructor
  // - waiting_locks: runtime-only field
  // - client_waiting_lock_counts: runtime-only field
  f->dump_int("held_locks", held_locks.size());
  for (auto &p : held_locks) {
    f->open_object_section("lock");
    f->dump_int("start", p.second.start);
    f->dump_int("length", p.second.length);
    f->dump_int("client", p.second.client);
    f->dump_int("owner", p.second.owner);
    f->dump_int("pid", p.second.pid);
    f->dump_int("type", p.second.type);
    f->close_section();
  }
  f->dump_int("client_held_lock_counts", client_held_lock_counts.size());
  for (auto &p : client_held_lock_counts) {
    f->open_object_section("client");
    f->dump_int("client_id", p.first.v);
    f->dump_int("count", p.second);
    f->close_section();
  }
}


std::vector<ceph_lock_state_t> ceph_lock_state_t::generate_test_instances() {
  std::vector<ceph_lock_state_t> ls;
  ls.push_back(ceph_lock_state_t(NULL, 0));
  ls.push_back(ceph_lock_state_t(NULL, 1));
  ls.back().held_locks.insert(std::make_pair(1, ceph_filelock()));
  ls.back().waiting_locks.insert(std::make_pair(1, ceph_filelock()));
  ls.back().client_held_lock_counts.insert(std::make_pair(1, 1));
  ls.back().client_waiting_lock_counts.insert(std::make_pair(1, 1));
  return ls;
}

bool ceph_lock_state_t::is_waiting(const ceph_filelock &fl) const
{
  auto p = waiting_locks.lower_bound(fl.start);
  while (p != waiting_locks.end()) {
    if (p->second.start > fl.start)
      return false;
    if (p->second.length == fl.length &&
	ceph_filelock_owner_equal(p->second, fl))
      return true;
    ++p;
  }
  return false;
}

void ceph_lock_state_t::remove_waiting(const ceph_filelock& fl)
{
  for (auto p = waiting_locks.lower_bound(fl.start);
       p != waiting_locks.end(); ) {
    if (p->second.start > fl.start)
      break;
    if (p->second.length == fl.length &&
	ceph_filelock_owner_equal(p->second, fl)) {
      if (type == CEPH_LOCK_FCNTL) {
	remove_global_waiting(p->second, this);
      }
      waiting_locks.erase(p);
      --client_waiting_lock_counts[(client_t)fl.client];
      if (!client_waiting_lock_counts[(client_t)fl.client]) {
        client_waiting_lock_counts.erase((client_t)fl.client);
      }
      break;
    }
    ++p;
  }
}

bool ceph_lock_state_t::is_deadlock(const ceph_filelock& fl,
                                    const lock_overlaps& overlapping_locks,
                                    const ceph_filelock *first_fl,
                                    unsigned depth) const
{
  ldout(cct,15) << "is_deadlock " << fl << dendl;

  // only for posix lock
  if (type != CEPH_LOCK_FCNTL)
    return false;

  // find conflict locks' owners
  std::set<ceph_filelock> lock_owners;
  for (const auto current : overlapping_locks.other) {
    if (fl.type == CEPH_LOCK_SHARED &&
	current->second.type == CEPH_LOCK_SHARED)
      continue;

    // circle detected
    if (first_fl && ceph_filelock_owner_equal(*first_fl, current->second)) {
      ldout(cct,15) << " detect deadlock" << dendl;
      return true;
    }

    ceph_filelock tmp = current->second;
    tmp.start = 0;
    tmp.length = 0;
    tmp.type = 0;
    lock_owners.insert(tmp);
  }

  if (depth >= MAX_DEADLK_DEPTH)
    return false;

  first_fl = first_fl ? first_fl : &fl;
  for (auto p = lock_owners.begin();
       p != lock_owners.end();
       ++p) {
    ldout(cct,15) << " conflict lock owner " << *p << dendl;
    // if conflict lock' owner is waiting for other lock?
    for (auto q = global_waiting_locks.lower_bound(*p);
	 q != global_waiting_locks.end();
	 ++q) {
      if (!ceph_filelock_owner_equal(q->first, *p))
	break;

      ceph_lock_state_t& state = *(q->second);
      auto overlaps = state.get_overlapping_locks(q->first);

      if (!std::empty(overlaps.other) &&
          is_deadlock(q->first, overlaps, first_fl, 1 + depth)) {
        return true;
      }
    }
  }
  return false;
}

void ceph_lock_state_t::add_waiting(const ceph_filelock& fl)
{
  waiting_locks.insert(pair<uint64_t, ceph_filelock>(fl.start, fl));
  ++client_waiting_lock_counts[(client_t)fl.client];
  if (type == CEPH_LOCK_FCNTL) {
    global_waiting_locks.insert(pair<ceph_filelock,ceph_lock_state_t*>(fl, this));
  }
}

bool ceph_lock_state_t::add_lock(ceph_filelock& new_lock,
                                 bool wait_on_fail, bool replay,
				 bool *deadlock)
{
  ldout(cct,15) << "add_lock " << new_lock << dendl;
  auto overlaps = get_overlapping_locks(new_lock, true);

  const auto wait_for_lock = [&] {
    if (!wait_on_fail || replay) {
      return;
    }

    if (is_deadlock(new_lock, overlaps)) {
      *deadlock = true;
      return;
    }

    add_waiting(new_lock);
  };

  if (!std::empty(overlaps.other)) {
    if (CEPH_LOCK_EXCL == new_lock.type) {
      ldout(cct,15) << "overlapping lock, and this lock is exclusive, can't set"
                    << dendl;
      wait_for_lock();
      return false;
    }

    if (overlaps.find_exclusive()) {
      ldout(cct,15) << " blocked by exclusive lock in overlapping_locks"
                    << dendl;
      wait_for_lock();
      return false;
    }
  }

  remove_waiting(new_lock);
  adjust_locks(overlaps, new_lock);
  ldout(cct,15) << "no conflicts, inserting " << new_lock << dendl;
  held_locks.insert(pair<uint64_t, ceph_filelock>(new_lock.start, new_lock));
  ++client_held_lock_counts[(client_t)new_lock.client];

  return true;
}

void ceph_lock_state_t::look_for_lock(ceph_filelock& testing_lock)
{
  const auto overlaps = get_overlapping_locks(testing_lock);

  if (!std::empty(overlaps.other)) {
    if (CEPH_LOCK_EXCL == testing_lock.type) { //any lock blocks it
      testing_lock = overlaps.other.front()->second;
      return;
    }

    if (const auto blocking_lock = overlaps.find_exclusive()) {
      testing_lock = blocking_lock->get();
      return;
    }
  }

  testing_lock.type = CEPH_LOCK_UNLOCK;
}

void ceph_lock_state_t::remove_lock(ceph_filelock removal_lock)
{
  auto overlaps = get_overlapping_locks(removal_lock);

  if (std::empty(overlaps.other) && std::empty(overlaps.owned)) {
    ldout(cct,15) << "attempt to remove lock at " << removal_lock.start
                  << " but no locks there!" << dendl;
  }

  bool remove_to_end = (0 == removal_lock.length);
  uint64_t removal_start = removal_lock.start;
  uint64_t removal_end = removal_start + removal_lock.length - 1;
  __s64 old_lock_client = 0;
  ceph_filelock *old_lock;

  ldout(cct,15) << "examining " << std::size(overlaps.owned)
          << " self-overlapping locks for removal" << dendl;
  for (const auto current : overlaps.owned) {
    ldout(cct,15) << "self overlapping lock " << current->second << dendl;
    old_lock = &current->second;
    bool old_lock_to_end = (0 == old_lock->length);
    uint64_t old_lock_end = old_lock->start + old_lock->length - 1;
    old_lock_client = old_lock->client;
    if (remove_to_end) {
      if (old_lock->start < removal_start) {
        old_lock->length = removal_start - old_lock->start;
      } else {
        ldout(cct,15) << "erasing " << current->second << dendl;
        held_locks.erase(current);
        --client_held_lock_counts[old_lock_client];
      }
    } else if (old_lock_to_end) {
      ceph_filelock append_lock = *old_lock;
      append_lock.start = removal_end+1;
      held_locks.insert(pair<uint64_t, ceph_filelock>
                        (append_lock.start, append_lock));
      ++client_held_lock_counts[(client_t)old_lock->client];
      if (old_lock->start >= removal_start) {
        ldout(cct,15) << "erasing " << current->second << dendl;
        held_locks.erase(current);
        --client_held_lock_counts[old_lock_client];
      } else old_lock->length = removal_start - old_lock->start;
    } else {
      if (old_lock_end  > removal_end) {
        ceph_filelock append_lock = *old_lock;
        append_lock.start = removal_end + 1;
        append_lock.length = old_lock_end - append_lock.start + 1;
        held_locks.insert(pair<uint64_t, ceph_filelock>
                          (append_lock.start, append_lock));
        ++client_held_lock_counts[(client_t)old_lock->client];
      }
      if (old_lock->start < removal_start) {
        old_lock->length = removal_start - old_lock->start;
      } else {
        ldout(cct,15) << "erasing " << current->second << dendl;
        held_locks.erase(current);
        --client_held_lock_counts[old_lock_client];
      }
    }
    if (!client_held_lock_counts[old_lock_client]) {
      client_held_lock_counts.erase(old_lock_client);
    }
  }
}

bool ceph_lock_state_t::remove_all_from (client_t client)
{
  bool cleared_any = false;
  if (client_held_lock_counts.count(client)) {
    multimap<uint64_t, ceph_filelock>::iterator iter = held_locks.begin();
    while (iter != held_locks.end()) {
      if ((client_t)iter->second.client == client) {
	held_locks.erase(iter++);
      } else
	++iter;
    }
    client_held_lock_counts.erase(client);
    cleared_any = true;
  }

  if (client_waiting_lock_counts.count(client)) {
    multimap<uint64_t, ceph_filelock>::iterator iter = waiting_locks.begin();
    while (iter != waiting_locks.end()) {
      if ((client_t)iter->second.client != client) {
	++iter;
	continue;
      }
      if (type == CEPH_LOCK_FCNTL) {
	remove_global_waiting(iter->second, this);
      }
      waiting_locks.erase(iter++);
    }
    client_waiting_lock_counts.erase(client);
  }
  return cleared_any;
}

void ceph_lock_state_t::adjust_locks(const lock_overlaps& overlaps,
                                     ceph_filelock& new_lock)
{
  ldout(cct,15) << "adjust_locks" << dendl;
  bool new_lock_to_end = (0 == new_lock.length);
  __s64 old_lock_client = 0;
  ceph_filelock *old_lock;
  for (const auto current : overlaps.owned) {
    old_lock = &current->second;
    ldout(cct,15) << "adjusting lock: " << *old_lock << dendl;
    bool old_lock_to_end = (0 == old_lock->length);
    uint64_t old_lock_start = old_lock->start;
    uint64_t old_lock_end = old_lock->start + old_lock->length - 1;
    uint64_t new_lock_start = new_lock.start;
    uint64_t new_lock_end = new_lock.start + new_lock.length - 1;
    old_lock_client = old_lock->client;
    if (new_lock_to_end || old_lock_to_end) {
      //special code path to deal with a length set at 0
      ldout(cct,15) << "one lock extends forever" << dendl;
      if (old_lock->type == new_lock.type) {
        //just unify them in new lock, remove old lock
        ldout(cct,15) << "same lock type, unifying" << dendl;
        new_lock.start = (new_lock_start < old_lock_start) ? new_lock_start :
          old_lock_start;
        new_lock.length = 0;
        held_locks.erase(current);
        --client_held_lock_counts[old_lock_client];
      } else { //not same type, have to keep any remains of old lock around
        ldout(cct,15) << "shrinking old lock" << dendl;
        if (new_lock_to_end) {
          if (old_lock_start < new_lock_start) {
            old_lock->length = new_lock_start - old_lock_start;
          } else {
            held_locks.erase(current);
            --client_held_lock_counts[old_lock_client];
          }
        } else { //old lock extends past end of new lock
          ceph_filelock appended_lock = *old_lock;
          appended_lock.start = new_lock_end + 1;
          held_locks.insert(pair<uint64_t, ceph_filelock>
                            (appended_lock.start, appended_lock));
          ++client_held_lock_counts[(client_t)old_lock->client];
          if (old_lock_start < new_lock_start) {
            old_lock->length = new_lock_start - old_lock_start;
          } else {
            held_locks.erase(current);
            --client_held_lock_counts[old_lock_client];
          }
        }
      }
    } else {
      if (old_lock->type == new_lock.type) { //just merge them!
        ldout(cct,15) << "merging locks, they're the same type" << dendl;
        new_lock.start = (old_lock_start < new_lock_start ) ? old_lock_start :
          new_lock_start;
        uint64_t new_end = (new_lock_end > old_lock_end) ? new_lock_end :
          old_lock_end;
        new_lock.length = new_end - new_lock.start + 1;
        ldout(cct,15) << "erasing lock " << current->second << dendl;
        held_locks.erase(current);
        --client_held_lock_counts[old_lock_client];
      } else { //we'll have to update sizes and maybe make new locks
        ldout(cct,15) << "locks aren't same type, changing sizes" << dendl;
        if (old_lock_end > new_lock_end) { //add extra lock after new_lock
          ceph_filelock appended_lock = *old_lock;
          appended_lock.start = new_lock_end + 1;
          appended_lock.length = old_lock_end - appended_lock.start + 1;
          held_locks.insert(pair<uint64_t, ceph_filelock>
                            (appended_lock.start, appended_lock));
          ++client_held_lock_counts[(client_t)old_lock->client];
        }
        if (old_lock_start < new_lock_start) {
          old_lock->length = new_lock_start - old_lock_start;
        } else { //old_lock starts inside new_lock, so remove it
          //if it extended past new_lock_end it's been replaced
          held_locks.erase(current);
          --client_held_lock_counts[old_lock_client];
        }
      }
    }
    if (!client_held_lock_counts[old_lock_client]) {
      client_held_lock_counts.erase(old_lock_client);
    }
  }

  //make sure to coalesce neighboring locks
  for (const auto current : overlaps.neighbors) {
    old_lock = &current->second;
    old_lock_client = old_lock->client;
    ldout(cct,15) << "lock to coalesce: " << *old_lock << dendl;
    /* because if it's a neighboring lock there can't be any self-overlapping
       locks that covered it */
    if (old_lock->type == new_lock.type) { //merge them
      if (0 == new_lock.length) {
        if (old_lock->start + old_lock->length == new_lock.start) {
          new_lock.start = old_lock->start;
        } else ceph_abort(); /* if there's no end to new_lock, the neighbor
                             HAS TO be to left side */
      } else if (0 == old_lock->length) {
        if (new_lock.start + new_lock.length == old_lock->start) {
          new_lock.length = 0;
        } else ceph_abort(); //same as before, but reversed
      } else {
        if (old_lock->start + old_lock->length == new_lock.start) {
          new_lock.start = old_lock->start;
          new_lock.length = old_lock->length + new_lock.length;
        } else if (new_lock.start + new_lock.length == old_lock->start) {
          new_lock.length = old_lock->length + new_lock.length;
        }
      }
      held_locks.erase(current);
      --client_held_lock_counts[old_lock_client];
    }
    if (!client_held_lock_counts[old_lock_client]) {
      client_held_lock_counts.erase(old_lock_client);
    }
  }
}

auto ceph_lock_state_t::get_last_before(uint64_t last_offset, lock_map& locks)
  -> lock_iterator
{
  auto last = locks.upper_bound(last_offset);

  if (last != std::begin(locks)) {
    --last;
  }

  if (std::end(locks) == last) {
    ldout(cct,15) << "get_last_before returning end()" << dendl;
    return last;
  }

  ldout(cct,15) << "get_last_before returning iterator pointing to "
                 << last->second << dendl;

  return last;
}

bool ceph_lock_state_t::share_space(const lock_iterator& iter,
                                    uint64_t start, uint64_t last_offset)
{
  bool ret = ((iter->first >= start && iter->first <= last_offset) ||
              ((iter->first < start) &&
               (((iter->first + iter->second.length - 1) >= start) ||
                (0 == iter->second.length))));

  ldout(cct,15) << "share_space got start: " << start << ", end: "
          << last_offset
          << ", lock: " << iter->second << ", returning " << ret << dendl;
  return ret;
}

auto ceph_lock_state_t::get_overlapping_locks(
  const ceph_filelock& requested_lock, const bool collect_neighbors)
  -> lock_overlaps
{
  ldout(cct,15) << "get_overlapping_locks" << dendl;
  lock_overlaps overlaps;

  // create a lock starting one earlier and ending one later
  // to check for neighbors
  ceph_filelock neighbor_check_lock = requested_lock;
  if (neighbor_check_lock.start != 0) {
    neighbor_check_lock.start = neighbor_check_lock.start - 1;
    if (neighbor_check_lock.length)
      neighbor_check_lock.length = neighbor_check_lock.length + 2;
  } else {
    if (neighbor_check_lock.length)
      neighbor_check_lock.length = neighbor_check_lock.length + 1;
  }
  //find the last held lock starting at the point after lock
  uint64_t endpoint = requested_lock.start;
  if (requested_lock.length) {
    endpoint += requested_lock.length;
  } else {
    endpoint = uint64_t(-1); // max offset
  }
  auto iter = get_last_before(endpoint, held_locks);

  while (iter != std::end(held_locks)) {
    const auto overlaps_requested_range = share_space(iter, requested_lock);

    if (overlaps_requested_range) {
      auto& matching_locks = ceph_filelock_owner_equal(
        requested_lock, iter->second)
        ? overlaps.owned : overlaps.other;
      matching_locks.push_back(iter);
    }

    if (!overlaps_requested_range && collect_neighbors &&
	ceph_filelock_owner_equal(neighbor_check_lock, iter->second) &&
        share_space(iter, neighbor_check_lock)) {
      overlaps.neighbors.push_back(iter);
    }

    if ((iter->first < requested_lock.start) &&
        (CEPH_LOCK_EXCL == iter->second.type)) {
      //can't be any more overlapping locks or they'd interfere with this one
      break;
    }

    if (std::begin(held_locks) == iter) {
      break;
    }

    --iter;
  }

  std::ranges::reverse(overlaps.other);
  std::ranges::reverse(overlaps.owned);
  std::ranges::reverse(overlaps.neighbors);

  return overlaps;
}

std::ostream& operator<<(std::ostream &out, const ceph_lock_state_t &l) {
  out << "ceph_lock_state_t. held_locks.size()=" << l.held_locks.size()
      << ", waiting_locks.size()=" << l.waiting_locks.size()
      << ", client_held_lock_counts -- " << l.client_held_lock_counts
      << "\n client_waiting_lock_counts -- " << l.client_waiting_lock_counts
      << "\n held_locks -- ";
    for (auto iter = l.held_locks.begin();
         iter != l.held_locks.end();
         ++iter)
      out << iter->second;
    out << "\n waiting_locks -- ";
    for (auto iter =l.waiting_locks.begin();
         iter != l.waiting_locks.end();
         ++iter)
      out << iter->second << "\n";
  return out;
}
