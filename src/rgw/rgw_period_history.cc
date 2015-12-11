// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_period_history.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw period history: ")

/// value comparison for avl_set
bool operator<(const RGWPeriodHistory::History& lhs,
               const RGWPeriodHistory::History& rhs)
{
  return lhs.get_newest_epoch() < rhs.get_newest_epoch();
}

/// key-value comparison for avl_set
struct NewestEpochLess {
  bool operator()(const RGWPeriodHistory::History& value, epoch_t key) const {
    return value.get_newest_epoch() < key;
  }
};


RGWPeriodHistory::RGWPeriodHistory(CephContext* cct, Puller* puller,
                                   const RGWPeriod& current_period)
  : cct(cct),
    puller(puller),
    current_epoch(current_period.get_realm_epoch())
{
  // copy the current period into a new history
  auto history = new History;
  history->periods.push_back(current_period);

  // insert as our current history
  current_history = histories.insert(*history).first;
}

RGWPeriodHistory::~RGWPeriodHistory()
{
  // clear the histories and delete each entry
  histories.clear_and_dispose(std::default_delete<History>{});
}

using Cursor = RGWPeriodHistory::Cursor;

Cursor RGWPeriodHistory::get_current() const
{
  return Cursor{current_history, &mutex, current_epoch};
}

Cursor RGWPeriodHistory::attach(RGWPeriod&& period)
{
  const auto epoch = period.get_realm_epoch();

  std::string predecessor_id;
  for (;;) {
    {
      // hold the lock over insert, and while accessing the unsafe cursor
      std::lock_guard<std::mutex> lock(mutex);

      auto cursor = insert_locked(std::move(period));
      if (!cursor) {
        return cursor;
      }
      if (current_history->contains(epoch)) {
        break; // the history is complete
      }

      // take the predecessor id of the most recent history
      if (cursor.get_epoch() > current_epoch) {
        predecessor_id = cursor.history->get_predecessor_id();
      } else {
        predecessor_id = current_history->get_predecessor_id();
      }
    }

    if (predecessor_id.empty()) {
      lderr(cct) << "reached a period with an empty predecessor id" << dendl;
      return Cursor{-EINVAL};
    }

    // pull the period outside of the lock
    int r = puller->pull(predecessor_id, period);
    if (r < 0) {
      return Cursor{r};
    }
  }

  // return a cursor to the requested period
  return Cursor{current_history, &mutex, epoch};
}

Cursor RGWPeriodHistory::insert(RGWPeriod&& period)
{
  std::lock_guard<std::mutex> lock(mutex);

  auto cursor = insert_locked(std::move(period));

  if (cursor.get_error()) {
    return cursor;
  }
  // we can only provide cursors that are safe to use outside of the mutex if
  // they're within the current_history, because other histories can disappear
  // in a merge. see merge() for the special handling of current_history
  if (cursor.history == current_history) {
    return cursor;
  }
  return Cursor{};
}

Cursor RGWPeriodHistory::lookup(epoch_t realm_epoch)
{
  if (current_history->contains(realm_epoch)) {
    return Cursor{current_history, &mutex, realm_epoch};
  }
  return Cursor{};
}

Cursor RGWPeriodHistory::insert_locked(RGWPeriod&& period)
{
  auto epoch = period.get_realm_epoch();

  // find the first history whose newest epoch comes at or after this period
  auto i = histories.lower_bound(epoch, NewestEpochLess{});

  if (i == histories.end()) {
    // epoch is past the end of our newest history
    auto last = --Set::iterator{i}; // last = i - 1

    if (epoch == last->get_newest_epoch() + 1) {
      // insert at the back of the last history
      last->periods.emplace_back(std::move(period));
      return Cursor{last, &mutex, epoch};
    }

    // create a new history for this period
    auto history = new History;
    history->periods.emplace_back(std::move(period));
    histories.insert(last, *history);

    i = Set::s_iterator_to(*history);
    return Cursor{i, &mutex, epoch};
  }

  if (i->contains(epoch)) {
    // already resident in this history
    auto& existing = i->get(epoch);
    // verify that the period ids match; otherwise we've forked the history
    if (period.get_id() != existing.get_id()) {
      lderr(cct) << "Got two different periods, " << period.get_id()
          << " and " << existing.get_id() << ", with the same realm epoch "
          << epoch << "! This indicates a fork in the period history." << dendl;
      return Cursor{-EEXIST};
    }
    // update the existing period if we got a newer period epoch
    if (period.get_epoch() > existing.get_epoch()) {
      existing = std::move(period);
    }
    return Cursor{i, &mutex, epoch};
  }

  if (epoch + 1 == i->get_oldest_epoch()) {
    // insert at the front of this history
    i->periods.emplace_front(std::move(period));

    // try to merge with the previous history
    if (i != histories.begin()) {
      auto prev = --Set::iterator{i};
      if (epoch == prev->get_newest_epoch() + 1) {
        i = merge(prev, i);
      }
    }
    return Cursor{i, &mutex, epoch};
  }

  if (i != histories.begin()) {
    auto prev = --Set::iterator{i};
    if (epoch == prev->get_newest_epoch() + 1) {
      // insert at the back of the previous history
      prev->periods.emplace_back(std::move(period));
      return Cursor{prev, &mutex, epoch};
    }
  }

  // create a new history for this period
  auto history = new History;
  history->periods.emplace_back(std::move(period));
  histories.insert(i, *history);

  i = Set::s_iterator_to(*history);
  return Cursor{i, &mutex, epoch};
}

RGWPeriodHistory::Set::iterator RGWPeriodHistory::merge(Set::iterator dst,
                                                        Set::iterator src)
{
  assert(dst->get_newest_epoch() + 1 == src->get_oldest_epoch());

  // always merge into current_history
  if (src == current_history) {
    // move the periods from dst onto the front of src
    src->periods.insert(src->periods.begin(),
                        std::make_move_iterator(dst->periods.begin()),
                        std::make_move_iterator(dst->periods.end()));
    histories.erase_and_dispose(dst, std::default_delete<History>{});
    return src;
  }

  // move the periods from src onto the end of dst
  dst->periods.insert(dst->periods.end(),
                      std::make_move_iterator(src->periods.begin()),
                      std::make_move_iterator(src->periods.end()));
  histories.erase_and_dispose(src, std::default_delete<History>{});
  return dst;
}


epoch_t RGWPeriodHistory::History::get_oldest_epoch() const
{
  return periods.front().get_realm_epoch();
}

epoch_t RGWPeriodHistory::History::get_newest_epoch() const
{
  return periods.back().get_realm_epoch();
}

bool RGWPeriodHistory::History::contains(epoch_t epoch) const
{
  return get_oldest_epoch() <= epoch && epoch <= get_newest_epoch();
}

RGWPeriod& RGWPeriodHistory::History::get(epoch_t epoch)
{
  return periods[epoch - get_oldest_epoch()];
}

const RGWPeriod& RGWPeriodHistory::History::get(epoch_t epoch) const
{
  return periods[epoch - get_oldest_epoch()];
}

const std::string& RGWPeriodHistory::History::get_predecessor_id() const
{
  return periods.front().get_predecessor();
}
