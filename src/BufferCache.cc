#include "BufferCache.h"
#include "include/ceph_assert.h"

#include <unistd.h>
#include <iostream>

#define LIBCEPHSQLITE_FLUSH_INTERVAL_SECONDS            5
#define LIBCEPHSQLITE_DIRTY_PAGE_CACHE_TIMEOUT_SECONDS  5

extern "C" void *write_back_thread(void *arg);

bool operator > (struct timespec lts, struct timespec rts)
{
  if (lts.tv_sec > rts.tv_sec)
    return true;

  if (lts.tv_sec == rts.tv_sec) {
    if (lts.tv_nsec > rts.tv_nsec)
      return true;
  }
  return false;
}


/* "greater than" comparison to float up the least recently used item
 * to the top of the Priority Queue.
 * "dirty" items should sink in the Priority Queue
 */
bool libcephsqlite::buffer_t_cmp_gt::operator () (libcephsqlite::buffer_t *l, libcephsqlite::buffer_t *r)
{
  if ((l->dirty && r->dirty) || (!l->dirty && !r->dirty)) {
    if (l->m_last_touch_time > r->m_last_touch_time)
      return true;
    return false;
  }
  if (l->dirty && !r->dirty)
    return true;
  return false;
}

libcephsqlite::BufferCache::BufferCache(const char *db_name, libradosstriper::RadosStriper *rs) :
  m_db_name(db_name),
  m_rs(rs)
{
  set_queue_names();
}

// page size must be power of 2
libcephsqlite::BufferCache::BufferCache(
  const char *db_name,
  libradosstriper::RadosStriper *rs,
  long cache_max_pages,
  int page_size) :
  m_db_name(db_name),
  m_rs(rs),
  m_page_size(page_size),
  m_cache_max_pages(cache_max_pages)
{
  set_queue_names();
}

libcephsqlite::BufferCache::~BufferCache()
{
  shutdown();
  for (auto it = m_cache_map.begin(); it != m_cache_map.end(); ++it) {
    delete it->second;
  }
}

void libcephsqlite::BufferCache::set_queue_names()
{
  std::string q_name;

  q_name = m_db_name;
  q_name += "-lru";
  m_lru_ipq.set_name(q_name.c_str());

  q_name = m_db_name;
  q_name += "-dirty";
  m_dirty_ipq.set_name(q_name.c_str());
}

void libcephsqlite::BufferCache::set_page_size(unsigned page_size)
{
  if (!m_inserted)
    m_page_size = page_size;
}

/* all pages are m_page_size in length */
bool libcephsqlite::BufferCache::write(void *buf, long offset, long len, bool dirty, bool &merged_dirty_range)
{
  if (m_shutting_down) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "shutting down!" << std::endl;
    return false;
  }

  bool new_page = false;
  bool reclaimed_page = false;
  long _off = offset;
  long old_off = -1;

  if ((_off % m_page_size) != 0)
    _off &= ~(m_page_size - 1); // get the actual page aligned offset for search

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "got:" << offset << ", need:" << _off << std::endl;

  lock();
  buffer_t *_b = m_cache_map[_off];

  if (!_b) {
    if (m_cache_curr_pages < m_cache_max_pages) {
      void *page = malloc(m_page_size);
      if (!page) {
        goto replace_page;
        // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "failed to malloc page" << std::endl;
        // return false;
      }
      _b = new buffer_t(page, _off, m_page_size);
      /* If we're out of memory before we hit the configured cache capacity,
       * should we gracefully handle this event by replacing a page from the
       * existing set ?
       */
      if (!_b) {
        free(page);
        goto replace_page;
        // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "failed to alloc buffer_t" << std::endl;
      }
      new_page = true;
      m_cache_curr_pages++;

      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "new page:" << _b << std::endl;
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "miss@" << offset << std::endl;
    } else {
replace_page:
      bool report_error = false;
      std::vector<libcephsqlite::buffer_t*> bufvec;
get_another_buffer:
      _b = m_lru_ipq.pop();

      if (!_b) {
        report_error = true;
        goto restore_buffers_in_lru_queue;
      }

      old_off = _b->offset;

      if (_b->dirty) {
        if (flush_page(this, _b) != 0) {
          // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::" << "reclaimed page:" << _b << ", flush FALIED!" << std::endl;
          bufvec.push_back(_b);
          goto get_another_buffer;
        }
        m_dirty_ipq.erase(_b);
      }

      _b->clear();
      _b->set_offset(_off);

      reclaimed_page = true;

restore_buffers_in_lru_queue:
      for (auto it: bufvec) {
        m_lru_ipq.push(it);
      }

      if (report_error)
        return false;
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "reclaimed page:" << _b << std::endl;
    }
  } else {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "hit@" << offset << std::endl;
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "old page:" << _b << std::endl;

    if (_b->dirty)
      m_dirty_ipq.erase(_b);
  }

  if (dirty) {
    if (!_b->dirty) {
      if (!m_dirty_ipq.find(_b))
        m_dirty_count++;
    }
  }

  _b->copy_range(buf, offset, len, dirty, merged_dirty_range);

  if (new_page) {
    m_cache_map[_off] = _b;
  } else {
    if (reclaimed_page) {
      m_cache_map.erase(old_off);
      m_cache_map[_off] = _b;
    } else {
      m_lru_ipq.erase(_b);
    }
  }
  m_lru_ipq.push(_b);
  if (_b->dirty)
    m_dirty_ipq.push(_b);
  unlock();

  return true;
}

bool libcephsqlite::BufferCache::read(void *buf, long offset, long len)
{
  if (m_shutting_down) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "shutting down!" << std::endl;
    return false;
  }

  long _off = offset;

  ceph_assert(len <= m_page_size);

  if ((_off % m_page_size) != 0)
    _off &= ~(m_page_size - 1); // get the actual page aligned offset for search

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "got:" << offset << ", need:" << _off << std::endl;

  lock();
  if (!m_cache_map[_off]) {
    unlock();
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "miss@" << offset << std::endl;
    return false;
  }

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "hit@" << offset << std::endl;
  buffer_t *_b = m_cache_map[_off];
  /* if we don't have the complete range, then return NULL (not found)
   * NOTE:
   * ideally there should be only one range for the entire page for
   * best performance
   */
  if (!_b->find_range(offset, len)) {
    unlock();
    return false;
  }

  void *srcbuf = (static_cast<char*>(_b->buf) + (offset % m_page_size));
  memcpy(buf, srcbuf, len);
  _b->update_usage_timestamp();
  m_lru_ipq.erase(_b);
  m_lru_ipq.push(_b);
  unlock();
  return true;
}

bool libcephsqlite::BufferCache::start()
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "starting thread for file:" << m_db_name << std::endl;
  return (pthread_create(&m_tid, NULL, write_back_thread, this) == 0);
}

bool libcephsqlite::BufferCache::shutdown()
{
  m_shutting_down = true; // this should be noticed by the write_back thread
  if (m_tid)
    pthread_join(m_tid, NULL);
  return true;
}

void libcephsqlite::BufferCache::dump()
{
  lock();
  for (auto it = m_cache_map.begin(); it != m_cache_map.end(); ++it) {
    libcephsqlite::buffer_t *b = it->second;
    long o = it->first;

    std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::dec << "key:" << o << "(" << b->len << ", " << b->offset << ")" << std::endl;
    b->dump();
  }
  m_lru_ipq.dump();
  unlock();
}

int libcephsqlite::flush_page(libcephsqlite::BufferCache *bc, libcephsqlite::buffer_t *b)
{
  bool is_write_pending = false;

  for (auto it = b->valid_ranges.begin(); it != b->valid_ranges.end(); ++it) {
    long write_offset = it->first - b->offset;
    long write_len    = it->second;

    /* ceph::bufferlist buffer pointers are all char* */
    ceph::bufferlist bl = ceph::bufferlist::static_from_mem(static_cast<char*>(b->buf) + write_offset, write_len);
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "writing(" << b->buf << ", len:" << std::dec << b->len << ", offset:" << b->offset << ")" << std::endl;
    if (bc->m_rs->write(bc->m_db_name, bl, write_len, b->offset + write_offset) != 0) {
      is_write_pending = true;
    }
  }
  if (!is_write_pending) {
    b->dirty = false;
    bc->m_dirty_count--;
    return 0;
  }
  return -1;
}

void libcephsqlite::flush_pages(libcephsqlite::BufferCache *bc, bool bwait)
{
  bool done = false;
  unsigned pages_flushed = 0;
  std::vector<libcephsqlite::buffer_t*> unflushed_pages;

  while (!done && pages_flushed < 50 && !bc->m_dirty_ipq.empty()) {
    libcephsqlite::buffer_t *b = bc->m_dirty_ipq.pop();
    
    //b->m.lock();
    ceph_assert(b->dirty);
    
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    // flush buffer only after 5 seconds of inactivity
    if (bwait && (now.tv_sec - b->m_last_touch_time.tv_sec) <
        LIBCEPHSQLITE_DIRTY_PAGE_CACHE_TIMEOUT_SECONDS) {
      /* if the popped buffer has timestamp within the last 5 seconds,
       * then there's no need to process other items in the dirty queue
       * since they will be retrieved in ascending order of their timestamps
       * which will be more closer to current time
       */
      bc->m_dirty_ipq.push(b);
      done = true;
      goto unlock_me;
    }
    
    if (flush_page(bc, b) == 0) {
      bc->m_lru_ipq.erase(b);
      bc->m_lru_ipq.push(b);
      pages_flushed++;
    } else {
      /* NOTE:
       * if we re-push the unflushed buffer back on the dirty queue
       * it'll be popped out immediately on the next iteration, since
       * the timestamp is not updated and the item will float to the
       * top leading to an infinite loop
       */
      unflushed_pages.push_back(b);
    }
unlock_me:
    //b->m.unlock();
    ;;
  }
  // re-push the unflushed pages back on the dirty queue
  for (auto it: unflushed_pages) {
    bc->m_dirty_ipq.push(it);
  }
  // std::cerr << std::hex << "0x" << pthread_self() << ":" << __FUNCTION__ << std::dec << ":: pages_flushed:" << pages_flushed << ", pending dirty count:" << bc->m_dirty_count << ", dirty queue count:" << bc->m_dirty_ipq.size() << std::endl;
}

void *libcephsqlite::write_back_thread(void *buffer_cache)
{
  libcephsqlite::BufferCache *bc = static_cast<libcephsqlite::BufferCache*>(buffer_cache);
  long s = 0;

  while (!bc->m_shutting_down) {
    usleep(100000);
    s += 100000;
    if ((s/100000) < LIBCEPHSQLITE_FLUSH_INTERVAL_SECONDS)
      continue; // check for shutdown flag approx. every 100 miliseconds

    s = 0;

    // flush 50 pages every 500 miliseconds
    bc->lock();

    if (bc->m_dirty_count)
      flush_pages(bc, true);

    bc->unlock();

    /* sleep: so that the sqlite interface thread gets a chance to run
     * instead of this thread spinning quickly in the 100us sleep iterations
     * at the top
     * NOTE:
     * pthread_yield() doesn't help us much to achieve the thread yielding
     */
    sleep(1);
  }
  bc->lock();
  while (bc->m_dirty_count) {
    flush_pages(bc, false);
  }
  ceph_assert(bc->m_dirty_count == 0);
  bc->unlock();
  return NULL;
}


/* assumption: valid_ranges are sorted by offset
 * NOTE:
 * If buffer is dirty and new copy is not flagged as dirty copy, then we need
 * to merge the new contents into the buffer contents. This call is mostly a
 * read-in when the required contents are not entirely in the buffer cache.
 * If new copy is marked as dirty, then its just an overwrite of buffer
 * contents with merginng of valid buffer spans (valid_ranges).
 */
void libcephsqlite::buffer_t::copy_range(void *_buf, long _offset, long _length, bool _dirty, bool &merged_dirty_buffer)
{
  // std::cerr << __FUNCTION__ << ":" << std::dec << __LINE__ << ": copying range:(length:" << _length << ", offset:" << _offset << ")" << std::endl;
  long write_offset = (_offset % this->len);

  /* part of the page may be dirty;
   * we don't overwrite the flag if a read is being done;
   * inserts during read should not be dirty;
   */
  //m.lock();
  if (this->dirty && !_dirty) {
    merged_dirty_buffer = true;
    /* _l should be zero at the end of the merge iterations in case we are
     * merging page patches
     */
    long _l = _length;
    long _o = _offset;
    /* We need to merge the contents carefully: i.e. copy over only the missing
     * patches which are *not* already in the cache.
     */
    // int i = 0;
    for (auto it = valid_ranges.begin(); it != valid_ranges.end(); ++it) {
      long merge_length = -1;

      if (_o < it->first) {
        if ((_o + _l) < it->first) {
          merge_length = _l;
          _l = 0;
          write_offset = _o % this->len;
        } else {
          merge_length = it->first - _o;
          write_offset = _o % this->len;
          if ((_o + _l) <= (it->first + it->second)) {
            /* nothing to do on next iteration */
            _l = 0;
          } else {
            _l -= merge_length;
            _o += merge_length;
          }
        }
        if (merge_length > 0) {
          memcpy((static_cast<char*>(buf) + write_offset), _buf, merge_length);
          // std::cerr << __FUNCTION__ << ":" << std::dec << __LINE__ << ": #" << ++i << ": memcpy(length:" << merge_length << ", offset:" << write_offset << std::endl;
        } else {
          ceph_assert(merge_length == 0);
        }
      } else if (_o == it->first) {
        if ((_o + _l) <= (it->first + it->second)) {
          /* nothing to do on the next iteration */
          _l = 0;
        } else {
          /* there nothing to merge though
           * we're just skipping over the matched range
           */
          _l -= it->second;
          _o += it->second;
        }
      } else if ((_o > it->first) && (_o < (it->first + it->second))) {
        if ((_o + _l) <= (it->first + it->second)) {
          /* nothing to do on the next iteration */
          _l = 0;
        } else {
          _l -= it->first + it->second - _o;
          _o = it->first + it->second;
        }
      }
      /* the pending range beyond the last valid range will be copied after 
       * all the iterations.
       */
    }
    if (_l != 0) {
      ceph_assert(_l < this->len);
      write_offset = _o % this->len;
      long merge_length = _l;
      ceph_assert((write_offset + merge_length) <= this->len);
      memcpy((static_cast<char*>(buf) + write_offset), _buf, merge_length);
      // std::cerr << __FUNCTION__ << ":" << std::dec << __LINE__ << ": #" << ++i << ": memcpy(length:" << merge_length << ", offset:" << write_offset << ")" << std::endl;
      _l = 0;
    }
    ceph_assert(_l == 0);
  } else {
    /* just overwrite */
    memcpy((static_cast<char*>(buf) + write_offset), _buf, _length);
    // std::cerr << __FUNCTION__ << ":" << std::dec << __LINE__ << ": ##" << ": memcpy(length:" << _length << ", offset:" << write_offset << ")" << std::endl;
  }

  // NOTE: reads need not cause cache to clear the dirty flag
  if (_dirty)
    this->dirty = _dirty;  // reads need not cause cache to clear the dirty flag

  ceph_assert((_offset & ~(len - 1)) == offset);
  ceph_assert(_length <= this->len);
  /* Merge range:
   * repeat until no more valid ranges can be found
   */
  while (true) {
    // std::cerr << std::dec << std::endl;
    // merge the new range into existing valid ranges in the set
    auto it = valid_ranges.begin();
    for (; it != valid_ranges.end(); ++it) {
      if (it->first == _offset) {
        if ((it->first + it->second) > (_offset + _length)) {
          _length = it->second;
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: A1 - merged:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        } else {
          /* delete current item and insert the new range */
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: A2 - add new:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        }
      } else if (it->first < _offset) {
        if ((it->first + it->second) < _offset) {
            /* not adjacent; not overlapping */
            /* nothing to do */
          // std::cerr << __FUNCTION__ << ":: B1 - skipped:(" << _length << ", " << _offset << ")" << std::endl;
        } else if ((it->first + it->second) == _offset) {
          /* found adjacent range */
          _offset = it->first;
          _length += it->second;
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: B2 - merged:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        } else if ((it->first + it->second) < (_offset + _length)) {
          _offset = it->first;
          _length = it->second + (_offset + _length - (it->first + it->second));
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: C - merged:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        } else if ((it->first + it->second) == (_offset + _length)) {
          _offset = it->first;
          _length = it->second;
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: D - merged:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        } else if ((it->first + it->second) > (_offset + _length)) {
          _offset = it->first;
          _length = it->second;
          valid_ranges.erase(it);
          // std::cerr << __FUNCTION__ << ":: E - merged:(" << _length << ", " << _offset << ")" << std::endl;
          break;
        } else {
          /* should not reach here */
          ceph_assert(0);
        }
      } else if (it->first > (_offset + _length)) {
        /* not an adjacent range
         * nothing to do
         * item from set lies beyond the new item span
         */
        // std::cerr << __FUNCTION__ << ":: F - skipped:(" << _length << ", " << _offset << ")" << std::endl;
      } else if (it->first == (_offset + _length)) {
        /* merge adjacent ranges */
        _offset = _offset;
        _length += it->second;
        valid_ranges.erase(it);
        // std::cerr << __FUNCTION__ << ":: G - merged:(" << _length << ", " << _offset << ")" << std::endl;
        break;
      } else if ((it->first > _offset) && (it->first < (_offset + _length))) {
        /* start offset of item from set lies within the new item range
         * but greater than the _offset
         */
        if ((it->first + it->second) <= (_offset + _length)) {
          _offset = _offset;
          _length = _length;
          // std::cerr << __FUNCTION__ << ":: H1 - merged:(" << _length << ", " << _offset << ")" << std::endl;
        } else {
          /* end point of item from set lies beyond the new item boundary */
          _offset = _offset;
          _length += (it->first + it->second - (_offset + _length));
          // std::cerr << __FUNCTION__ << ":: H2 - merged:(" << _length << ", " << _offset << ")" << std::endl;
        }
        valid_ranges.erase(it);
        break;
      }
    }
    if (it == valid_ranges.end())
      break;
  }

  auto it = valid_ranges.begin();
  while (it != valid_ranges.end()) {
    if (it->first > _offset)
      break;
    ++it;
  }

  // std::cerr << __FUNCTION__ << ":" << std::dec << __LINE__ << ": adding range:(length:" << _length << ", offset:" << _offset << ")" << std::endl;

  ceph_assert((_offset & ~(this->len - 1)) == this->offset);
  ceph_assert(_length <= this->len);
  ceph_assert((_offset + _length) <= (this->offset + this->len));

  valid_ranges.insert(it, std::make_pair(_offset, _length));
  update_usage_timestamp();
  //m.unlock();
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "added range:(length:" << _length << ", offset:" << _offset << ")" << std::endl;
  // std::cerr << "----------" << std::endl;
}

// find range with 100% overlap
bool libcephsqlite::buffer_t::find_range(long _offset, long _length)
{
  for (auto it = valid_ranges.begin(); it != valid_ranges.end(); ++it) {
    if ((_offset >= it->first) && ((_offset + _length) <= (it->first + it->second)))
      return true;
  }
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "no range found:(length:" << _length << ", offset:" << _offset << ")" << std::endl;
  return false;
}

void libcephsqlite::buffer_t::update_usage_timestamp()
{
  clock_gettime(CLOCK_MONOTONIC, &m_last_touch_time);
}

void libcephsqlite::buffer_t::clear()
{
  //memset(buf, 0, len);
  offset = -1;
  dirty = false;
  m_last_touch_time.tv_sec = 0;
  m_last_touch_time.tv_nsec = 0;
  valid_ranges.clear();
}

void libcephsqlite::buffer_t::dump()
{
  std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::hex << this << std::dec << ", dirty:" << dirty << ", last used:" << m_last_touch_time.tv_sec << "." << std::setw(9) << std::setfill('0') << m_last_touch_time.tv_nsec << std::endl;
  // reset field width and fill chars
  std::cout << std::setw(0) << std::setfill('\0');
  for (auto it = valid_ranges.begin(); it != valid_ranges.end(); ++it) {
    std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "    (length:" << it->second << ", offset:" << it->first << ")" << std::endl;
  }
}

