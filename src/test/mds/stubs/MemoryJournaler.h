#pragma once

#include <sys/errno.h>
#include "osdc/Journaler.h"
#include "common/Finisher.h"

#include <map>
#include <list>

class MemoryJournaler: public Journaler {
  using entries_t = std::map<uint64_t, bufferlist>;
  using entries_i = entries_t::iterator;

  // the last entry should hold
  // (write_pos, empty bufferlist)
  entries_t entries;
  entries_i read_it, safe_it;
  Header header;

  uint64_t expire_pos;
  std::map<uint64_t, std::list<Context*>> safe_waiters;

  bool readonly = true;

  Finisher* finisher;
  void complete(Context *c, int r = 0) {
    if (c) {
      if (finisher) {
        finisher->queue(c);
      } else {
        c->complete(r);
      }
    }
  }
public:

  MemoryJournaler(Finisher *finisher = nullptr): finisher(finisher) { }

  /* reset
   *
   * NOTE: we assume the caller knows/has ensured that any objects in
   * our sequence do not exist.. e.g. after a MKFS.  this is _not_ an
   * "erase" method.
   */
  void reset()
  {
    entries.clear();
    read_it = safe_it = entries.end();
    header.trimmed_pos = 0;
    header.expire_pos = 0;
    header.unused_field = 0;
    header.write_pos = 0;

    expire_pos = header.layout.get_period(); // aka write_pos in this case
    entries[expire_pos] = bufferlist(); // empty bufferlist;
    safe_it = entries.begin();
    read_it = entries.begin();

    readonly = true;
  }

  void create(file_layout_t const* layout, stream_format_t const sf)
  {
    header.layout = *layout;
    header.stream_format = sf;
    reset();
  }

  bool try_read_entry(bufferlist& bl)
  {
    // the distance to the end should be >= 2
    // because the last element in the map is the write pos
    if (std::distance(read_it, entries.end()) < 2)
      return false;

    bl.swap(read_it->second);
    read_it++;
  
    return true;
  }
  uint64_t append_entry(bufferlist& bl) {
    auto write_it = std::prev(entries.end());
    assert(write_it->second.length() == 0);
    uint64_t new_write_pos = write_it->first + bl.length();
    write_it->second.swap(bl);
    entries[new_write_pos] = bufferlist();
    return new_write_pos;
  }
  void trim() {
    auto trim_to_it = entries.lower_bound(header.expire_pos);
    entries.erase(entries.begin(), trim_to_it);
  }
  void trim_tail() {
    trim();
  }
  /**
   * Cause any ongoing waits to error out with -EAGAIN, set error
   * to -EAGAIN.
   */
  void shutdown() {}

  // Asynchronous operations
  // =======================
  void erase(Context* completion) {
    reset();
    complete(completion);
  }
  void recover(Context* onfinish) {
    assert(entries.size());

    expire_pos = header.expire_pos;
    set_read_pos(expire_pos);
    set_write_pos(header.write_pos);

    complete(onfinish);
  }
  void reread_head(Context* onfinish) {
    recover(onfinish);
  }
  void reread_head_and_probe(Context* onfinish) {
    recover(onfinish);
  }
  void write_head(Context* onsave = 0) {
    header.expire_pos = expire_pos;
    header.unused_field = entries.lower_bound(expire_pos)->first;
    header.write_pos = safe_it->first;
    header.trimmed_pos = entries.begin()->first;
    complete(onsave);
  }
  void wait_for_flush(Context* onsafe = 0) {
    if (std::distance(safe_it, entries.end()) < 2) {
      complete(onsafe);
    } else if (onsafe) {
      safe_waiters[get_write_pos()].push_back(onsafe);
    }
  }
  void flush(Context* onsafe = 0) {
    safe_it = std::prev(entries.end());
    while (safe_waiters.size() && safe_waiters.begin()->first <= safe_it->first) {
      for(auto ctx : safe_waiters.begin()->second) {
        complete(ctx);
      }
      safe_waiters.erase(safe_waiters.begin());
    }
  }
  void wait_for_readable(Context* onfinish) {
    // memory journaler implementation is always readable
    complete(onfinish);
  }
  void wait_for_prezero(Context* onfinish) {
    complete(onfinish);
  }

  // Synchronous setters
  // ===================
  void set_layout(file_layout_t const* l) {
    header.layout = *l;
  }
  void set_readonly() {
    readonly = true;
  }
  void set_writeable() {
    readonly = false;
  }
  void set_write_pos(uint64_t p) {
    auto write_it = entries.lower_bound(p);
    entries.erase(write_it, entries.end());
    write_it = entries.insert({header.write_pos, bufferlist()}).first;
    safe_it = write_it;
  }
  void set_read_pos(uint64_t p) {
    read_it = entries.lower_bound(p);
    assert(read_it != entries.end());
  }
  void set_expire_pos(uint64_t ep) {
    expire_pos = ep;
  }
  void set_trimmed_pos(uint64_t p) {
    auto it = entries.lower_bound(p);
    assert(it != entries.end());
    entries.erase(entries.begin(), it);
  }
  void set_write_error_handler(Context* c) { }
  void set_write_iohint(uint32_t iohint_flags) { }

  // Synchronous getters
  // ===================
  bool have_waiter() const { return false; }
  uint64_t get_layout_period() const { return header.layout.get_period(); }
  const file_layout_t& get_layout() const { return header.layout; }
  uint32_t get_stream_format() const { return header.stream_format; }
  bool is_active() const { return entries.size() > 0; }
  bool is_stopping() const { return false; }
  int get_error() const { return 0; }
  bool is_readonly() const { return readonly; }

  bool is_readable() { return true; }
  bool is_write_head_needed() const { 
    return false
     || header.write_pos != get_write_pos()
     || header.expire_pos != expire_pos
     || header.trimmed_pos != entries.begin()->first;
  }
  uint64_t get_write_pos() const {
    assert(entries.size());
    return entries.rbegin()->first;
  }
  uint64_t get_write_safe_pos() const {
    if (safe_it == entries.end()) {
      return 0;
    }
    return safe_it->first;
  }
  uint64_t get_read_pos() const {
    if (read_it == entries.end()) {
      return 0;
    }
    return read_it->first;
  }
  uint64_t get_expire_pos() const {
    return expire_pos;
  }
  uint64_t get_trimmed_pos() const {
    if (entries.size() == 0) {
      return 0;
    }
    return entries.begin()->first;
  }
  size_t get_journal_envelope_size() const {
    if (header.stream_format >= StreamFormat::JOURNAL_FORMAT_RESILIENT) {
      return JOURNAL_ENVELOPE_RESILIENT;
    } else {
      return JOURNAL_ENVELOPE_LEGACY;
    }
  }
};