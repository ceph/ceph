#pragma once

#include <cstdint>
#include <stdexcept>
#include <algorithm>
#include <iterator>
#include <concepts>

#include "include/Context.h"
#include "include/ceph_assert.h"
#include "include/mempool.h"
#include "include/types.h"

#include "mdstypes.h"

struct WaitTag {
  static constexpr const int MASK_BITS = 48;
  static constexpr const int ID_BITS = 16;
  static constexpr const uint16_t ANY_ID = 0xffff;

  static_assert(MASK_BITS + ID_BITS == 64);

  static constexpr const uint64_t FULL_MASK = (uint64_t(1) << MASK_BITS)-1;

  union {
    uint64_t raw;
    struct {
      uint64_t mask : MASK_BITS;
      uint64_t id : ID_BITS;
    };
  };

  constexpr WaitTag(uint16_t id = ANY_ID)
      : WaitTag(id, 0)
  {
  }
  constexpr WaitTag(uint16_t id, uint64_t mask)
      : mask(mask)
      , id(id)
  {
    if ((mask & FULL_MASK) != mask) {
      throw std::logic_error("mask too large");
    }
  }

  WaitTag(WaitTag const& other) : raw(other.raw) { }

  constexpr static WaitTag const  from_raw(uint64_t raw) {
    return WaitTag(raw >> MASK_BITS, raw & FULL_MASK);
  }

  constexpr static WaitTag const any(uint64_t mask = 0) {
    return WaitTag(ANY_ID, mask);
  }

  constexpr WaitTag with_mask(uint64_t new_mask) const
  {
    return WaitTag(id, new_mask);
  }

  constexpr WaitTag or_mask(uint64_t or_mask) const
  {
    return WaitTag(id, mask | or_mask);
  }

  constexpr WaitTag and_mask(uint64_t and_mask) const
  {
    return WaitTag(id, mask & and_mask);
  }

  constexpr WaitTag bit_mask(int bit) const
  {
    return or_mask(uint64_t(1) << bit);
  }

  constexpr bool match(uint64_t raw) const
  {
    return match(from_raw(raw));
  }

  constexpr bool match(WaitTag const& other) const
  {
    return match_id(other) && (mask & other.mask);
  }

  constexpr bool match_id(WaitTag const& other) const
  {
    return (id == other.id || is_any_id() || other.is_any_id());
  }

  constexpr auto operator<=>(WaitTag const& other) const {
    return raw <=> other.raw;
  }

  constexpr operator bool() const {
    return mask != 0;
  }

  constexpr bool is_any_id() const {
    return id == ANY_ID;
  }
};

constexpr WaitTag operator~(WaitTag const& tag) {
  return WaitTag(tag.id, ~tag.mask & WaitTag::FULL_MASK);
}

constexpr WaitTag operator|(WaitTag const& l, uint64_t mask) {
  return l.or_mask(mask);
}

constexpr WaitTag operator|(WaitTag const& l, WaitTag const&r) {
  if (!l.match_id(r)) {
    throw std::logic_error("Can't combine tags with different IDs");
  }
  return WaitTag(std::min(l.id, r.id), l.mask | r.mask);
}

constexpr WaitTag operator&(WaitTag const& l, WaitTag const&r) {
  if (!l.match_id(r)) {
    throw std::logic_error("Can't combine tags with different IDs");
  }
  return WaitTag(std::min(l.id, r.id), l.mask & r.mask);
}

constexpr WaitTag operator&(WaitTag const& l, uint64_t mask) {
  return l.and_mask(mask);
}

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const WaitTag& tag)
{
  os << "WaitTag(";
  if (tag.is_any_id()) {
    os << "*";
  } else {
    os << tag.id;
  }
  os << ":" << std::hex << tag.mask << std::dec << ")";
  return os;
}


template<std::derived_from<Context> C>
struct MDSWaitable {
  virtual void add_waiter(WaitTag tag, C* c, bool ordered = false)
  {
    uint64_t seq = 0;
    if (ordered) {
      seq = ++last_wait_seq;
    }
    waiting.insert({ tag, { seq, c } });
  }

  bool has_waiter_for(WaitTag tag)
  {
    auto it = MatchingIterator(waiting, tag);
    return it != waiting.end();
  }

  template <std::output_iterator<C*> S> 
  void take_waiting(WaitTag tag, S &&sink)
  {
    if (waiting.empty())
      return;

    // process ordered waiters in the same order that they were added.
    std::map<uint64_t, C*> ordered_waiters;

    for (auto it = MatchingIterator(waiting, tag); it != waiting.end();) {
      if (it->second.first > 0) {
        ordered_waiters.insert(it->second);
      } else {
        *sink++ = it->second.second;
      }
      waiting.erase(it++);
    }
    for (auto it = ordered_waiters.begin(); it != ordered_waiters.end(); ++it) {
      *sink++ = it->second;
    }
  }

  bool waiting_empty() const {
    return waiting.empty();
  }

  void waiting_clear() {
    waiting.clear();
  }

  virtual ~MDSWaitable() { }

  MDSWaitable() = default;
  MDSWaitable(MDSWaitable<C> const& other) = default;
  MDSWaitable(MDSWaitable<C> && other) = default;

private:
  using Waiters = mempool::mds_co::compact_multimap<WaitTag, std::pair<uint64_t, C*>>;
  Waiters waiting;
  uint64_t last_wait_seq = 0;

  struct MatchingIterator {
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<const WaitTag, std::pair<uint64_t, C*>>;
    using pointer = value_type*;
    using reference = value_type&;

    Waiters & parent;
    WaitTag tag;
    typename Waiters::iterator current;

    /// @brief Reduces any value to its most significant bit
    /// @param value 
    /// @return a new value that has a single bit set at the position
    ///         of the input's msb
    static uint64_t reduce_to_msb(uint64_t value) {
      while (value & (value - 1)) // if more than one bit is set
        value &= value - 1; //  clear LSB
      return value;
    }

    MatchingIterator(Waiters& parent, WaitTag tag)
    : parent(parent)
    , tag(tag)
    {
      auto min_mask = reduce_to_msb(tag.mask);

      auto search_tag = WaitTag(tag.is_any_id() ? 0 : tag.id, min_mask);
      current = parent.lower_bound(search_tag);
      skip_to_matching();
    }

    MatchingIterator(MatchingIterator const& other) = default;

    void skip_to_matching() {
      while (true) {
        if (current == parent.end()) {
          return;
        }
        if (current->first.match(tag)) {
          return;
        }
        if (!current->first.is_any_id() && current->first.id > tag.id) {
          break;
        }
        ++current;
      }
      if (!tag.is_any_id()) {
        current = parent.lower_bound(WaitTag::any());
        skip_to_matching();
      }
    }

    reference operator*() const { return *current; }
    pointer operator->() { return current.operator->(); }

    // Prefix increment
    MatchingIterator& operator++()
    {
      ++current;
      skip_to_matching();
      return *this;
    }

    // Postfix increment
    MatchingIterator operator++(int)
    {
      MatchingIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    operator typename Waiters::iterator() { return current; }
    operator typename Waiters::const_iterator() { return Waiters::const_iterator(current); }

    friend bool operator==(const MatchingIterator& a, const MatchingIterator& b) { return a.current == b.current; };
    friend bool operator==(const MatchingIterator& a, const typename Waiters::const_iterator& b) { return a.current == b; };
    friend bool operator==(const MatchingIterator& a, const typename Waiters::iterator& b) { return a.current == b; };
    friend bool operator!=(const MatchingIterator& a, const MatchingIterator& b) { return a.current != b.current; };
    friend bool operator!=(const MatchingIterator& a, const typename Waiters::const_iterator& b) { return a.current != b; };
    friend bool operator!=(const MatchingIterator& a, const typename Waiters::iterator& b) { return a.current != b; };
  };
};
