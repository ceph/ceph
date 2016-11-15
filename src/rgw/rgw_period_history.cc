// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_period_history.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw period history: ")

/// an ordered history of consecutive periods
class RGWPeriodHistory::History : public bi::avl_set_base_hook<> {
 public:
  std::deque<RGWPeriod> periods;

  epoch_t get_oldest_epoch() const {
    return periods.front().get_realm_epoch();
  }
  epoch_t get_newest_epoch() const {
    return periods.back().get_realm_epoch();
  }
  bool contains(epoch_t epoch) const {
    return get_oldest_epoch() <= epoch && epoch <= get_newest_epoch();
  }
  RGWPeriod& get(epoch_t epoch) {
    return periods[epoch - get_oldest_epoch()];
  }
  const RGWPeriod& get(epoch_t epoch) const {
    return periods[epoch - get_oldest_epoch()];
  }
  const std::string& get_predecessor_id() const {
    return periods.front().get_predecessor();
  }
};

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


using Cursor = RGWPeriodHistory::Cursor;

const RGWPeriod& Cursor::get_period() const
{
  std::lock_guard<std::mutex> lock(*mutex);
  return history->get(epoch);
}
bool Cursor::has_prev() const
{
  std::lock_guard<std::mutex> lock(*mutex);
  return epoch > history->get_oldest_epoch();
}
bool Cursor::has_next() const
{
  std::lock_guard<std::mutex> lock(*mutex);
  return epoch < history->get_newest_epoch();
}


class RGWPeriodHistory::Impl final {
 public:
  Impl(CephContext* cct, Puller* puller, const RGWPeriod& current_period);
  ~Impl();

  Cursor get_current() const { return current_cursor; }
  Cursor attach(RGWPeriod&& period);
  Cursor insert(RGWPeriod&& period);
  Cursor lookup(epoch_t realm_epoch);

 private:
  /// an intrusive set of histories, ordered by their newest epoch. although
  /// the newest epoch of each history is mutable, the ordering cannot change
  /// because we prevent the histories from overlapping
  using Set = bi::avl_set<RGWPeriodHistory::History>;

  /// insert the given period into the period history, creating new unconnected
  /// histories or merging existing histories as necessary. expects the caller
  /// to hold a lock on mutex. returns a valid cursor regardless of whether it
  /// ends up in current_history, though cursors in other histories are only
  /// valid within the context of the lock
  Cursor insert_locked(RGWPeriod&& period);

  /// merge the periods from the src history onto the end of the dst history,
  /// and return an iterator to the merged history
  Set::iterator merge(Set::iterator dst, Set::iterator src);

  /// construct a Cursor object using Cursor's private constuctor
  Cursor make_cursor(Set::const_iterator history, epoch_t epoch);

  CephContext *const cct;
  Puller *const puller; //< interface for pulling missing periods
  Cursor current_cursor; //< Cursor to realm's current period

  mutable std::mutex mutex; //< protects the histories

  /// set of disjoint histories that are missing intermediate periods needed to
  /// connect them together
  Set histories;

  /// iterator to the history that contains the realm's current period
  Set::const_iterator current_history;
};

RGWPeriodHistory::Impl::Impl(CephContext* cct, Puller* puller,
                             const RGWPeriod& current_period)
  : cct(cct), puller(puller)
{
  if (!current_period.get_id().empty()) {
    // copy the current period into a new history
    auto history = new History;
    history->periods.push_back(current_period);

    // insert as our current history
    current_history = histories.insert(*history).first;

    // get a cursor to the current period
    current_cursor = make_cursor(current_history, current_period.get_realm_epoch());
  }
}

RGWPeriodHistory::Impl::~Impl()
{
  // clear the histories and delete each entry
  histories.clear_and_dispose(std::default_delete<History>{});
}

Cursor RGWPeriodHistory::Impl::attach(RGWPeriod&& period)
{
  if (current_history == histories.end()) {
    return Cursor{-EINVAL};
  }

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
      if (cursor.get_epoch() > current_cursor.get_epoch()) {
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
  return make_cursor(current_history, epoch);
}

Cursor RGWPeriodHistory::Impl::insert(RGWPeriod&& period)
{
  if (current_history == histories.end()) {
    return Cursor{-EINVAL};
  }

  std::lock_guard<std::mutex> lock(mutex);

  auto cursor = insert_locked(std::move(period));

  if (cursor.get_error()) {
    return cursor;
  }
  // we can only provide cursors that are safe to use outside of the mutex if
  // they're within the current_history, because other histories can disappear
  // in a merge. see merge() for the special handling of current_history
  if (cursor.history == &*current_history) {
    return cursor;
  }
  return Cursor{};
}

Cursor RGWPeriodHistory::Impl::lookup(epoch_t realm_epoch)
{
  if (current_history != histories.end() &&
      current_history->contains(realm_epoch)) {
    return make_cursor(current_history, realm_epoch);
  }
  return Cursor{};
}

Cursor RGWPeriodHistory::Impl::insert_locked(RGWPeriod&& period)
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
      return make_cursor(last, epoch);
    }

    // create a new history for this period
    auto history = new History;
    history->periods.emplace_back(std::move(period));
    histories.insert(last, *history);

    i = Set::s_iterator_to(*history);
    return make_cursor(i, epoch);
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
    return make_cursor(i, epoch);
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
    return make_cursor(i, epoch);
  }

  if (i != histories.begin()) {
    auto prev = --Set::iterator{i};
    if (epoch == prev->get_newest_epoch() + 1) {
      // insert at the back of the previous history
      prev->periods.emplace_back(std::move(period));
      return make_cursor(prev, epoch);
    }
  }

  // create a new history for this period
  auto history = new History;
  history->periods.emplace_back(std::move(period));
  histories.insert(i, *history);

  i = Set::s_iterator_to(*history);
  return make_cursor(i, epoch);
}

RGWPeriodHistory::Impl::Set::iterator
RGWPeriodHistory::Impl::merge(Set::iterator dst, Set::iterator src)
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

Cursor RGWPeriodHistory::Impl::make_cursor(Set::const_iterator history,
                                           epoch_t epoch) {
  return Cursor{&*history, &mutex, epoch};
}


RGWPeriodHistory::RGWPeriodHistory(CephContext* cct, Puller* puller,
                                   const RGWPeriod& current_period)
  : impl(new Impl(cct, puller, current_period)) {}

RGWPeriodHistory::~RGWPeriodHistory() = default;

Cursor RGWPeriodHistory::get_current() const
{
  return impl->get_current();
}
Cursor RGWPeriodHistory::attach(RGWPeriod&& period)
{
  return impl->attach(std::move(period));
}
Cursor RGWPeriodHistory::insert(RGWPeriod&& period)
{
  return impl->insert(std::move(period));
}
Cursor RGWPeriodHistory::lookup(epoch_t realm_epoch)
{
  return impl->lookup(realm_epoch);
}
