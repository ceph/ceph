// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/* EC "extent" cache.  This extent cache attempts to improve performance,
 * particularly for small sequential writes, by caching the results of recent
 * reads and writes.
 *
 * The cache has two parts: The main cache which is active while an IO is
 * outstanding to an object and an "LRU" which stashes recent IO according to
 * a least-recently-used scheme.
 *
 * The cache keeps all caches indexed by shard, shard_offset. That is it
 * independently tracks caches for each shard of an EC. It will keep a cache
 * even for shards which are currently offline or missing, since the cache
 * is formed from the result of reads and writes, which are required to always
 * calculate missing shards.
 *
 * The cache allows for a single read to be outstanding per PG at a time. If
 * multiple writes are received while a read is active, the next read will
 * contain all necessary reads, so as to catch up. Early on in development, a
 * more parallel read mechanism was explored but was found to have no benefit.
 *
 * This cache will never re-order IO.
 *
 * The LRU
 *
 * The LRU is a per-OSD-shard (not to be confused with an EC shard). Since the
 * OSD-shard can have multiple threads, the LRU must have a mutex. This should
 * not be required for crimson-based pools, since each osd shard has a single
 * reactor. Some effort has been made to limit the frequency that this mutex is
 * taken.
 *
 * The LRU has a maximum size (defined in the constructor) and will keep its
 * usage below this amount.
 *
 * Cache Lines
 *
 * The LRU tracks extents of recent writes with cache Lines.  These are
 * simple-to-track ranges of offsets across all shards. Each line represents 32K
 * of address space on each shard.
 *
 * A cache line can be owned by:
 * - No-one (i.e. it is not instantiated)
 * - Object - an IO is inflight for this cache line
 * - LRU - A recent IO touched this cache line.
 *
 * This simple ownership model means that the locking required for the LRU does
 * not leak out into the wider extent cache and allows for the entire cache
 * to be built from reference-counters.
 *
 * Client API
 *
 * The client has a number of required interactions:
 * 1. prepare(...). This creates a cache op. All cache ops required for a single
 *                  parent op must be prepared before any are executed.
 * 2. execute(...). Execute an IO. This gives the cache permission to perform
 *                  the IO. This function can (and frequently does) call back
 *                  re-entrantly, so the caller must be aware that this can
 *                  happen.
 *
 * The client must provide a mechanism for the extent cache to read. It does
 * this by extending the ECExtentCache::BackendRead class.
 *
 * Once a read is complete, the client must call cache.read_done().
 *
 * When the cache is ready, it will call back the lambda passed to execute.
 * The client is expected to populate the write data, including any parity
 * data, by calling the cache.write_done() method.
 *
 * Finally, there is an on_change() and on_change2() methods. The first of these
 * instructs the extent cache to discard any ops it has queued.  The second
 * simply asserts that the cache is now idle, this is to ensure that the calling
 * code has performed the required clean up to clear the extent cache.
 */

#pragma once

#include "ECUtil.h"
#include "include/Context.h"

class ECExtentCache {
  class Address;
  class Line;
  class Object;
  typedef std::shared_ptr<Line> LineRef;
  typedef std::list<LineRef>::iterator LineIter;

 public:
  class LRU;
  class Op;
  typedef std::shared_ptr<Op> OpRef;

  struct BackendReadListener {
    virtual void backend_read(hobject_t oid,
                              ECUtil::shard_extent_set_t const &request,
                              uint64_t object_size) = 0;
    virtual ~BackendReadListener() = default;
  };

  static void update_mempool(int items, int64_t bytes) {
    mempool::get_pool(mempool::pool_index_t(mempool::mempool_ec_extent_cache)).
        adjust_count(items, bytes);
  }

  class LRU {
   public:
    class Key {
     public:
      uint64_t offset;
      hobject_t oid;

      Key(uint64_t offset, const hobject_t &oid) : offset(offset), oid(oid) {};

      friend bool operator==(const Key &lhs, const Key &rhs) {
        return lhs.offset == rhs.offset
            && lhs.oid == rhs.oid;
      }

      friend bool operator!=(const Key &lhs, const Key &rhs) {
        return !(lhs == rhs);
      }
    };

    struct KeyHash {
      std::size_t operator()(const Key &obj) const {
        std::size_t seed = 0x625610ED;
        seed ^= (seed << 6) + (seed >> 2) + 0x1E665363 + static_cast<
          std::size_t>(obj.offset);
        seed ^= (seed << 6) + (seed >> 2) + 0x51343C80 + obj.oid.get_hash();
        return seed;
      }
    };

   private:
    friend class Object;
    friend class ECExtentCache;
    std::unordered_map<Key, std::pair<
                         std::list<Key>::iterator, std::shared_ptr<
                           ECUtil::shard_extent_map_t>>, KeyHash> map;
    std::list<Key> lru;
    uint64_t max_size = 0;
    uint64_t size = 0;
    ceph::mutex mutex = ceph::make_mutex("ECExtentCache::LRU");

    void free_maybe();
    void discard();
    void add(const Line &line);
    void erase(const Key &k);
    std::list<Key>::iterator erase(const std::list<Key>::iterator &it,
                                   bool update_mempool);
    std::shared_ptr<ECUtil::shard_extent_map_t> find(
        const hobject_t &oid, uint64_t offset);
    void remove_object(const hobject_t &oid);

   public:
    explicit LRU(uint64_t max_size) : map(), max_size(max_size) {}
  };

  class Op {
    friend class Object;
    friend class ECExtentCache;

    Object &object;
    std::optional<ECUtil::shard_extent_set_t> const reads;
    ECUtil::shard_extent_set_t const writes;
    ECUtil::shard_extent_map_t result;
    bool complete = false;
    bool invalidates_cache = false;
    bool did_invalidate_cache = false;
    bool reading = false;
    bool read_done = false;
    uint64_t projected_size = 0;
    GenContextURef<OpRef&> cache_ready_cb;
    std::list<LineRef> lines;

    // List of callbacks to be executed on write completion (not commit)
    std::list<std::function<void(void)>> on_write;

    const extent_set get_pin_eset(uint64_t alignment) const;

   public:
    explicit Op(
        GenContextURef<OpRef&> &&cache_ready_cb,
        Object &object,
        std::optional<ECUtil::shard_extent_set_t> const &to_read,
        ECUtil::shard_extent_set_t const &write,
        uint64_t projected_size,
        bool invalidates_cache);

    ~Op();
    void cancel() { delete cache_ready_cb.release(); }
    const  ECUtil::shard_extent_set_t &get_writes() const { return writes; }
    const Object &get_object() const { return object; }
    const hobject_t &get_hoid() const { return object.oid; }
    const ECUtil::shard_extent_map_t &get_result() { return result; }

    void add_on_write(std::function<void(void)> &&cb) {
      on_write.emplace_back(std::move(cb));
    }

    bool complete_if_reads_cached(OpRef &op_ref) {
      if (!read_done) {
        return false;
      }
      result = object.get_cache(reads);
      complete = true;
      cache_ready_cb.release()->complete(op_ref);
      return true;
    }

    void write_done(ECUtil::shard_extent_map_t const &update) const {
      object.write_done(update, projected_size);
      for (auto &cb: on_write) {
        cb();
      }
    }
  };

#define MIN_LINE_SIZE (32UL*1024UL)

private:
  class Object {
    friend class Op;
    friend class LRU;
    friend class Line;
    friend class ECExtentCache;

    ECExtentCache &pg;
    ECUtil::shard_extent_set_t requesting;
    ECUtil::shard_extent_set_t do_not_read;
    std::list<OpRef> reading_ops;
    std::list<OpRef> requesting_ops;
    // Map of the byte-offset of the start of the line to the line.
    std::map<uint64_t, std::weak_ptr<Line>> lines;
    int active_ios = 0;
    uint64_t current_size = 0;
    uint64_t projected_size = 0;
    uint64_t line_size = 0;
    bool reading = false;
    bool cache_invalidate_expected = false;

    void request(OpRef &op);
    void send_reads();
    void unpin(Op &op) const;
    void delete_maybe() const;
    void erase_line(uint64_t offset);
    void invalidate(const OpRef &invalidating_op);

   public:
    hobject_t oid;

    Object(ECExtentCache &pg, hobject_t const &oid, uint64_t size) :
      pg(pg),
      requesting(pg.sinfo.get_k_plus_m()),
      do_not_read(pg.sinfo.get_k_plus_m()),
      current_size(size),
      projected_size(size),
      oid(oid) {
      line_size = std::max(MIN_LINE_SIZE, pg.sinfo.get_chunk_size());
    }

    void insert(ECUtil::shard_extent_map_t const &buffers) const;
    void write_done(ECUtil::shard_extent_map_t const &buffers, uint64_t new_size);
    void read_done(ECUtil::shard_extent_map_t const &result);
    [[nodiscard]] uint64_t get_projected_size() const { return projected_size; }
    ECUtil::shard_extent_map_t get_cache(
        std::optional<ECUtil::shard_extent_set_t> const &set) const;
    uint64_t line_align(uint64_t line) const;
  };


  class Line {
   public:
    uint64_t offset;
    uint64_t size;
    std::shared_ptr<ECUtil::shard_extent_map_t> cache;
    Object &object;

    Line(Object &object,
         uint64_t offset) :
      offset(offset),
      object(object) {
      std::shared_ptr<ECUtil::shard_extent_map_t> c = object.pg.lru.find(
        object.oid, offset);

      if (c == nullptr) {
        cache = std::make_shared<ECUtil::shard_extent_map_t>(&object.pg.sinfo);
        size = 0;
        /* We are creating an empty cache line */
        update_mempool(1, 0);
      } else {
        cache = c;
        size = c->size();
      }
    }

    ~Line() {
      object.pg.lru.add(*this);
      object.erase_line(offset);
    }

    friend bool operator==(const Line &lhs, const Line &rhs) {
      return lhs.offset == rhs.offset
          && lhs.object.oid == rhs.object.oid;
    }

    friend bool operator!=(const Line &lhs, const Line &rhs) {
      return !(lhs == rhs);
    }
  };

  std::map<hobject_t, Object> objects;
  BackendReadListener &backend_read;
  LRU &lru;
  const ECUtil::stripe_info_t &sinfo;
  std::list<OpRef> waiting_ops;
  void cache_maybe_ready();
  uint32_t active_ios = 0;
  CephContext *cct;

  OpRef prepare(GenContextURef<OpRef&> &&ctx,
                hobject_t const &oid,
                std::optional<ECUtil::shard_extent_set_t> const &to_read,
                ECUtil::shard_extent_set_t const &write,
                uint64_t orig_size,
                uint64_t projected_size,
                bool invalidates_cache);

 public:
  ~ECExtentCache() {
    // This should really only be needed in failed tests, as the PG should
    // clear up any IO before it gets destructed. However, here we make sure
    // to clean up any outstanding IO.
    on_change();
    on_change2();
  }

  explicit ECExtentCache(BackendReadListener &backend_read,
                         LRU &lru, const ECUtil::stripe_info_t &sinfo,
                         CephContext *cct
    ) :
    backend_read(backend_read),
    lru(lru),
    sinfo(sinfo),
    cct(cct) {}

  // Insert some data into the cache.
  void read_done(hobject_t const &oid, ECUtil::shard_extent_map_t const &update);
  void write_done(OpRef const &op, ECUtil::shard_extent_map_t const &update);
  void on_change();
  void on_change2() const;
  [[nodiscard]] bool contains_object(hobject_t const &oid) const;
  [[nodiscard]] uint64_t get_projected_size(hobject_t const &oid) const;

  template <typename CacheReadyCb>
  OpRef prepare(hobject_t const &oid,
                std::optional<ECUtil::shard_extent_set_t> const &to_read,
                ECUtil::shard_extent_set_t const &write,
                uint64_t orig_size,
                uint64_t projected_size,
                bool invalidates_cache,
                CacheReadyCb &&ready_cb) {
    GenContextURef<OpRef&> ctx =
        make_gen_lambda_context<OpRef&, CacheReadyCb>(
          std::forward<CacheReadyCb>(ready_cb));

    return prepare(std::move(ctx), oid, to_read, write, orig_size,
                   projected_size, invalidates_cache);
  }

  void execute(std::list<OpRef> &op_list);
  [[nodiscard]] bool idle() const;

  void add_on_write(std::function<void(void)> &&cb) const {
    if (waiting_ops.empty()) {
      cb();
    } else {
      waiting_ops.back()->add_on_write(std::move(cb));
    }
  }
}; // ECExtentCaches
