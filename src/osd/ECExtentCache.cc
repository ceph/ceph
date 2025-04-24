// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ECExtentCache.h"
#include "ECUtil.h"

#include <ranges>

using namespace std;
using namespace ECUtil;

void ECExtentCache::Object::request(OpRef &op) {
  /* Record that this object is invalidating cache, to avoid any further
   * read attempts (which will be discarded).
   */
  if (op->invalidates_cache) {
    cache_invalidate_expected = true;
  }

  extent_set eset = op->get_pin_eset(line_size);

  for (auto &&[start, len] : eset) {
    for (uint64_t to_pin = start; to_pin < start + len; to_pin += line_size) {
      LineRef l;
      if (!lines.contains(to_pin)) {
        l = make_shared<Line>(*this, to_pin);
        if (!l->cache->empty()) {
          l->cache->to_shard_extent_set(do_not_read);
        }
        lines.emplace(to_pin, weak_ptr(l));
      } else {
        l = lines.at(to_pin).lock();
      }
      op->lines.emplace_back(l);
    }
  }

  bool read_required = false;

  // If this op previously invalidate cache, the cache had better be empty.
  if (op->did_invalidate_cache) {
    ceph_assert(do_not_read.empty());
  }

  /* Deal with reads if there are any.
   * If any cache invalidation ops have been added, there is no point adding any
   * reads as they are all going to be thrown away before any of the
   * post-invalidate ops are honoured.
   */
  if (op->reads && !cache_invalidate_expected) {
    for (auto &&[shard, eset] : *(op->reads)) {
      extent_set request = eset;
      if (do_not_read.contains(shard)) {
        request.subtract(do_not_read.at(shard));
      }

      if (!request.empty()) {
        requesting[shard].union_of(request);
        read_required = true;
        requesting_ops.emplace_back(op);
      }
    }
  }


  /* Calculate the range of the object which no longer need to be written. This
   * will include:
   *  - Any reads being issued by this IO.
   *  - Any writes being issued (these will be cached)
   *  - any unwritten regions in an append - these can assumed to be zero.
   */
  if (read_required) {
    do_not_read.insert(requesting);
  }
  do_not_read.insert(op->writes);
  if (op->projected_size > projected_size) {
    /* This write is growing the size of the object. This essentially counts
     * as a write (although the cache will not get populated). Future reads
     * to this area will be skipped, but this makes them essentially zero
     * reads.
     */
    shard_extent_set_t obj_hole(pg.sinfo.get_k_plus_m());
    shard_extent_set_t read_mask(pg.sinfo.get_k_plus_m());

    pg.sinfo.ro_size_to_read_mask(op->projected_size, obj_hole);
    pg.sinfo.ro_size_to_read_mask(projected_size, read_mask);
    obj_hole.subtract(read_mask);
    do_not_read.insert(obj_hole);
  }

  projected_size = op->projected_size;

  if (read_required) {
    send_reads();
  }
  else {
    op->read_done = true;
  }
}

void ECExtentCache::Object::send_reads() {
  if (reading || requesting.empty())
    return; // Read busy

  reading_ops.swap(requesting_ops);
  pg.backend_read.backend_read(oid, requesting, current_size);
  requesting.clear();
  reading = true;
}

void ECExtentCache::Object::read_done(shard_extent_map_t const &buffers) {
  reading = false;
  for (auto &&op : reading_ops) {
    op->read_done = true;
  }
  reading_ops.clear();
  insert(buffers);
}

uint64_t ECExtentCache::Object::line_align(uint64_t x) const {
  return x - (x % line_size);
}

void ECExtentCache::Object::insert(shard_extent_map_t const &buffers) const {
  if (buffers.empty()) return;

  /* The following gets quite inefficient for writes which write to the start
   * and the end of a very large object, since we iterated over the middle.
   * This seems like a strange use case, so currently this is not being
   * optimised.
   */
  for (uint64_t slice_start = line_align(buffers.get_start_offset());
       slice_start < buffers.get_end_offset();
       slice_start += line_size) {
    shard_extent_map_t slice = buffers.slice_map(slice_start, line_size);
    if (!slice.empty()) {
      LineRef l = lines.at(slice_start).lock();
      /* The line should have been created already! */
      l->cache->insert(slice);
      uint64_t old_size = l->size;
      l->size = l->cache->size();
      ceph_assert(l->size >= old_size);
      update_mempool(0, l->size - old_size);
    }
  }
}

void ECExtentCache::Object::write_done(shard_extent_map_t const &buffers,
                                       uint64_t new_size) {
  insert(buffers);
  current_size = new_size;
}

void ECExtentCache::Object::unpin(Op &op) const {
  op.lines.clear();
  delete_maybe();
}

void ECExtentCache::Object::delete_maybe() const {
  if (lines.empty() && active_ios == 0) {
    pg.objects.erase(oid);
  }
}

void check_seset_empty_for_range(shard_extent_set_t s, uint64_t off,
                                 uint64_t len) {
  for (auto &[shard, eset] : s) {
    ceph_assert(!eset.intersects(off, len));
  }
}

void ECExtentCache::Object::erase_line(uint64_t offset) {
  check_seset_empty_for_range(requesting, offset, line_size);
  do_not_read.erase_stripe(offset, line_size);
  lines.erase(offset);
}

void ECExtentCache::Object::invalidate(const OpRef &invalidating_op) {
  for (auto &l : std::views::values(lines)) {
    auto line = l.lock();
    line->cache->clear();
    update_mempool(0, -line->size);
    line->size = 0;
  }

  // Remove all entries from the LRU
  pg.lru.remove_object(oid);

  ceph_assert(!reading);
  do_not_read.clear();
  requesting.clear();
  requesting_ops.clear();
  reading_ops.clear();

  /* Current size should reflect the actual size of the object, which was set
   * by the previous write. We are going to replay all the writes now, so set
   * the projected size to that of this op.
   */
  projected_size = invalidating_op->projected_size;

  // Invalidate cache has been honoured, so no need to repeat.
  invalidating_op->invalidates_cache = false;
  invalidating_op->did_invalidate_cache = true;

  cache_invalidate_expected = false;

  // We now need to reply all outstanding ops to regenerate the read
  for (auto &op : pg.waiting_ops) {
    if (op->object.oid == oid) {
      op->read_done = false;
      request(op);
    }
  }
}

void ECExtentCache::cache_maybe_ready() {
  while (!waiting_ops.empty()) {
    OpRef op = waiting_ops.front();
    if (op->invalidates_cache) {
      /* We must wait for any outstanding reads to complete. The cache replans
       * all reads as part of invalidate. If an in-flight read completes after
       * the invalidate, it will potentially corrupt it, leading to data
       * corruption at the host.
       */
      if (op->object.reading) {
        return;
      }
      op->object.invalidate(op);
      ceph_assert(!op->invalidates_cache);
    }
    /* If reads_done finds all reads complete it will call the completion
     * callback. Typically, this will cause the client to execute the
     * transaction and pop the front of waiting_ops.  So we abort if either
     * reads are not ready, or the client chooses not to complete the op
     */
    if (!op->complete_if_reads_cached(op)) {
      return;
    }

    waiting_ops.pop_front();
  }
}

ECExtentCache::OpRef ECExtentCache::prepare(GenContextURef<OpRef&> &&ctx,
                                            hobject_t const &oid,
                                            std::optional<shard_extent_set_t>
                                            const &to_read,
                                            shard_extent_set_t const &write,
                                            uint64_t orig_size,
                                            uint64_t projected_size,
                                            bool invalidates_cache) {

  auto object_iter = objects.find(oid);
  if (object_iter == objects.end()) {
    auto p = objects.emplace(oid, Object(*this, oid, orig_size));
    object_iter = p.first;
  }
  OpRef op = std::make_shared<Op>(
    std::move(ctx), object_iter->second, to_read, write, projected_size,
    invalidates_cache);

  return op;
}

void ECExtentCache::read_done(hobject_t const &oid,
                              shard_extent_map_t const &update) {
  objects.at(oid).read_done(update);
  cache_maybe_ready();
  objects.at(oid).send_reads();
}

void ECExtentCache::write_done(OpRef const &op,
                               shard_extent_map_t const &update) {
  op->write_done(std::move(update));
}

uint64_t ECExtentCache::get_projected_size(hobject_t const &oid) const {
  return objects.at(oid).get_projected_size();
}

bool ECExtentCache::contains_object(hobject_t const &oid) const {
  return objects.contains(oid);
}

ECExtentCache::Op::~Op() {
  ceph_assert(object.active_ios > 0);
  object.active_ios--;
  ceph_assert(object.pg.active_ios > 0);
  object.pg.active_ios--;

  object.unpin(*this);
}

/* ECExtent cache cleanup on occurs in two parts. The first performs cleanup
 * of the ops currently managed by the extent cache. At this point, however
 * the cache will be waiting for other parts of EC to clean up (for example
 * any outstanding reads). on_change2() executes once all of this cleanup has
 * occurred.
 */
void ECExtentCache::on_change() {
  for (auto &&o : std::views::values(objects)) {
    o.reading_ops.clear();
    o.requesting_ops.clear();
    o.requesting.clear();
  }
  for (auto &&op : waiting_ops) {
    op->cancel();
  }
  waiting_ops.clear();
}

/* This must be run toward the end of EC on_change handling.  It asserts that
 * any object which is automatically self-destructs when idle has done so.
 * Additionally, it discards the entire LRU cache. This must be done after all
 * in-flight reads/writes have completed, or we risk attempting to insert data
 * into the cache after it has been cleared.
 *
 * Note that the LRU will end up being called multiple times. With some
 * additional code complexity this could be fixed for a small (probably
 * insignificant) performance improvement.
 */
void ECExtentCache::on_change2() const {
  lru.discard();
  /* If this assert fires in a unit test, make sure that all ops have completed
   * and cleared any extent cache ops they contain */
  ceph_assert(objects.empty());
  ceph_assert(active_ios == 0);
  ceph_assert(idle());
}

void ECExtentCache::execute(list<OpRef> &op_list) {
  for (auto &op : op_list) {
    op->object.request(op);
  }
  waiting_ops.insert(waiting_ops.end(), op_list.begin(), op_list.end());
  cache_maybe_ready();
}

bool ECExtentCache::idle() const {
  return active_ios == 0;
}

list<ECExtentCache::LRU::Key>::iterator ECExtentCache::LRU::erase(
    const list<Key>::iterator &it,
    bool do_update_mempool) {
  uint64_t size_change = map.at(*it).second->size();
  if (do_update_mempool) {
    update_mempool(-1, 0 - size_change);
  }
  size -= size_change;
  size_t removed = map.erase(*it);
  ceph_assert(removed == 1);
  return lru.erase(it);
}

void ECExtentCache::LRU::add(const Line &line) {
  if (line.size == 0) {
    update_mempool(-1, 0);
    return;
  }

  const Key k(line.offset, line.object.oid);

  shared_ptr<shard_extent_map_t> cache = line.cache;

  mutex.lock();
  ceph_assert(!map.contains(k));
  auto i = lru.insert(lru.end(), k);
  auto j = make_pair(std::move(i), std::move(cache));
  map.insert(std::pair(std::move(k), std::move(j)));
  size += line.size; // This is already accounted for in mempool.
  free_maybe();
  mutex.unlock();
}

shared_ptr<shard_extent_map_t> ECExtentCache::LRU::find(
    const hobject_t &oid, uint64_t offset) {
  Key k(offset, oid);
  shared_ptr<shard_extent_map_t> cache = nullptr;
  mutex.lock();
  if (map.contains(k)) {
    auto &&[lru_iter, c] = map.at(k);
    cache = c;
    auto it = lru_iter; // Intentional copy.
    erase(it, false);
  }
  mutex.unlock();
  return cache;
}

void ECExtentCache::LRU::remove_object(const hobject_t &oid) {
  mutex.lock();
  for (auto it = lru.begin(); it != lru.end();) {
    if (it->oid == oid) {
      it = erase(it, true);
    } else {
      ++it;
    }
  }
  mutex.unlock();
}

void ECExtentCache::LRU::free_maybe() {
  while (max_size < size) {
    auto it = lru.begin();
    erase(it, true);
  }
}

void ECExtentCache::LRU::discard() {
  mutex.lock();
  lru.clear();
  update_mempool(0 - map.size(), 0 - size);
  map.clear();
  size = 0;
  mutex.unlock();
}

const extent_set ECExtentCache::Op::get_pin_eset(uint64_t alignment) const {
  extent_set eset = writes.get_extent_superset();
  if (reads) {
    reads->get_extent_superset(eset);
  }
  eset.align(alignment);

  return eset;
}

ECExtentCache::Op::Op(GenContextURef<OpRef&> &&cache_ready_cb,
                      Object &object,
                      std::optional<shard_extent_set_t> const &to_read,
                      shard_extent_set_t const &write,
                      uint64_t projected_size,
                      bool invalidates_cache) :
  object(object),
  reads(to_read),
  writes(write),
  result(&object.pg.sinfo),
  invalidates_cache(invalidates_cache),
  projected_size(projected_size),
  cache_ready_cb(std::move(cache_ready_cb)) {
  object.active_ios++;
  object.pg.active_ios++;
}

shard_extent_map_t ECExtentCache::Object::get_cache(
    std::optional<shard_extent_set_t> const &set) const {
  if (!set) {
    return shard_extent_map_t(&pg.sinfo);
  }

  shard_id_map<extent_map> res(pg.sinfo.get_k_plus_m());
  for (auto &&[shard, eset] : *set) {
    for (auto [off, len] : eset) {
      for (uint64_t slice_start = line_align(off);
           slice_start < off + len;
           slice_start += line_size) {
        uint64_t offset = max(slice_start, off);
        uint64_t length = min(slice_start + line_size, off + len) - offset;
        // This line must exist, as it was created when the op was created.
        LineRef l = lines.at(slice_start).lock();
        if (l->cache->contains_shard(shard)) {
          extent_map m = l->cache->get_extent_map(shard).intersect(
            offset, length);
          if (!m.empty()) {
            if (!res.contains(shard)) res.emplace(shard, std::move(m));
            else res.at(shard).insert(m);
          }
        }
      }
    }
  }
  return shard_extent_map_t(&pg.sinfo, std::move(res));
}
