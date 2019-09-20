#ifndef __BUFFER_CACHE_H__
#define __BUFFER_CACHE_H__

#include <string>
#include <map>
#include <functional>

#include <pthread.h>
#include <time.h>

#include "include/radosstriper/libradosstriper.hpp"
#include "common/ceph_mutex.h"
#include "IndexedPriorityQueue.h"


namespace libcephsqlite
{
  const unsigned int PAGE_SIZE = (8<<10);

  class BufferCache;
  struct buffer_t;

  int flush_page(libcephsqlite::BufferCache *bc, libcephsqlite::buffer_t *b);
  void flush_pages(libcephsqlite::BufferCache *bc, bool bwait);
  void *write_back_thread(void *buffer_cache);

  struct buffer_t {
    buffer_t(void *_buf, long _offset, long _length) :
      buf(_buf), offset(_offset), len(_length)
    {}

    ~buffer_t()
    {
      free(buf);
    }

    void set_offset(long _offset) { offset = _offset; }
    void copy_range(void *_buf, long offset, long length, bool dirty, bool &merged_dirty_range);
    bool find_range(long offset, long length);
    void update_usage_timestamp();
    void clear();
    void dump();

    ceph::mutex m = ceph::make_mutex("buffer_t::m");;
    void *buf = NULL;
    long  offset = -1;       // page offset
    long  len = -1;          // page length
    bool  dirty = false;
    struct timespec m_last_touch_time = {0, 0};
    // valid real (offset, length) pairs
    std::vector< std::pair<long, long> > valid_ranges;
  };

  struct buffer_t_cmp_gt {
    /* "greater than" comparison to float up the least recently used item
     * to the top of the Priority Queue.
     * "dirty" items should sink in the Priority Queue
     */
    bool operator () (libcephsqlite::buffer_t *l, libcephsqlite::buffer_t *r);
  };

  class BufferCache
  {
    public:
      BufferCache(const char *db_name, libradosstriper::RadosStriper *rs);
      BufferCache(const char *db_name, libradosstriper::RadosStriper *rs, long cache_max_pages, int page_size);
      ~BufferCache();

      void set_page_size(unsigned page_size);
      unsigned get_page_size() { return m_page_size; }
      bool write(void *buf, long offset, long len, bool dirty, bool &merged_dirty_range);
      bool read(void *buf, long offset, long len);
      bool start();
      bool shutdown();
      bool is_dirty() { return !m_dirty_ipq.empty(); }
      void lock() { m_cache_map_mutex.lock(); }
      void unlock() { m_cache_map_mutex.unlock(); }
      void dump();

    private:
      typedef std::map<long, buffer_t*> CacheMap;
      typedef IndexedPriorityQueue<libcephsqlite::buffer_t*, libcephsqlite::buffer_t_cmp_gt> BufferIPQ;

      bool m_inserted = false;
      bool m_shutting_down = false;
      const char *m_db_name = NULL;
      libradosstriper::RadosStriper *m_rs = NULL;
      unsigned m_page_size = libcephsqlite::PAGE_SIZE;
      long m_cache_curr_pages = 0;
      long m_cache_max_pages = (64<<10);
      ceph::mutex m_cache_map_mutex = ceph::make_mutex("libcephsqlite::BufferCache::m_cache_map_mutex");;
      CacheMap m_cache_map;
      BufferIPQ m_lru_ipq;
      BufferIPQ m_dirty_ipq;
      pthread_t m_tid = 0;
      int m_dirty_count = 0;

      void set_queue_names();

      friend int flush_page(libcephsqlite::BufferCache *bc, libcephsqlite::buffer_t *b);
      friend void flush_pages(libcephsqlite::BufferCache *bc, bool bwait);
      friend void *write_back_thread(void *buffer_cache);
  };
  
}
#endif
