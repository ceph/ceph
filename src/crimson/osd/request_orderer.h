// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <utility>

#include <seastar/core/semaphore.hh>

namespace crimson::osd {

// Register a turn synchronously, before releasing an upstream ordering stage.
// Different keys proceed independently. Entries live until their last turn is
// released, including turns whose acquisition future has not resumed yet.
template <typename Key>
class RequestOrderer {
  struct Entry {
    seastar::semaphore sem{1};
    size_t users = 0;
  };
  using entries_t = std::map<Key, Entry>;
  entries_t entries;

public:
  RequestOrderer() = default;
  RequestOrderer(const RequestOrderer&) = delete;
  RequestOrderer &operator=(const RequestOrderer&) = delete;

  class [[nodiscard("keep the turn until the ordered handoff is complete")]] Turn {
    RequestOrderer *owner;
    typename entries_t::iterator entry;
    seastar::semaphore_units<> units;

  public:
    Turn(RequestOrderer &owner, typename entries_t::iterator entry,
         seastar::semaphore_units<> units)
      : owner(&owner), entry(entry), units(std::move(units)) {}
    Turn(const Turn&) = delete;
    Turn(Turn &&other) noexcept
      : owner(std::exchange(other.owner, nullptr)), entry(other.entry),
        units(std::move(other.units)) {}
    ~Turn() { reset(); }

    void reset() {
      if (auto *orderer = std::exchange(owner, nullptr)) {
        units.return_all();
        if (--entry->second.users == 0) {
          orderer->entries.erase(entry);
        }
      }
    }
  };

  seastar::future<Turn> get(const Key &key) {
    auto entry = entries.try_emplace(key).first;
    ++entry->second.users;
    return seastar::get_units(entry->second.sem, 1).then(
      [this, entry](auto units) {
        return Turn{*this, entry, std::move(units)};
      });
  }
};

}
