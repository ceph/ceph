//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

#define dout_context cct
#define dout_subsys ceph_subsys_osd

void ECExtentCache::Object::request(OpRef &op)
{
  /* After a cache invalidation, we allow through a single cache-invalidating
   * IO.
   */
  if (op->invalidates_cache) {
    if (cache_invalidated) {
      op->invalidates_cache = false;
    } else {
      cache_invalidate_expected = true;
    }
  }
  cache_invalidated = false;

  extent_set eset = op->get_pin_eset(line_size);

  /* Manipulation of lines must take the mutex. */
  for (auto &&[start, len]: eset ) {
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

  /* Deal with reads if there are any.
   * If any cache invalidation ops have been added, there is no point adding any
   * reads as they are all going to be thrown away before any of the
   * post-invalidate ops are honoured.
   */
  if (op->reads && !cache_invalidate_expected) {
    for (auto &&[shard, eset]: *(op->reads)) {
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
  if (read_required) do_not_read.insert(requesting);
  do_not_read.insert(op->writes);
  if (op->projected_size > projected_size) {
    /* This write is growing the size of the object. This essentially counts
     * as a write (although the cache will not get populated). Future reads
     * to this area will be skipped, but this makes them essentially zero
     * reads.
     */
    shard_extent_set_t obj_hole(sinfo.get_k_plus_m());
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());

    sinfo.ro_size_to_read_mask(op->projected_size, obj_hole);
    sinfo.ro_size_to_read_mask(projected_size, read_mask);
    obj_hole.subtract(read_mask);
    do_not_read.insert(obj_hole);
  } else if (op->projected_size < projected_size) {
    // Invalidate the object's cache when we see any object reduce in size.
    op->invalidates_cache = true;
  }

  projected_size = op->projected_size;

  if (read_required) send_reads();
  else op->read_done = true;
}

void ECExtentCache::Object::send_reads()
{
  if (reading || requesting.empty())
    return; // Read busy

  reading_ops.swap(requesting_ops);
  pg.backend_read.backend_read(oid, requesting, current_size);
  requesting.clear();
  reading = true;
}

void ECExtentCache::Object::read_done(shard_extent_map_t const &buffers)
{
  reading = false;
  for (auto && op : reading_ops) {
    op->read_done = true;
  }
  reading_ops.clear();
  insert(buffers);
  send_reads();
}

uint64_t ECExtentCache::Object::line_align(uint64_t x) const
{
  return x - (x % line_size);
}

void ECExtentCache::Object::insert(shard_extent_map_t const &buffers)
{
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
    }
  }
}

void ECExtentCache::Object::write_done(shard_extent_map_t const &buffers, uint64_t new_size)
{
  insert(buffers);
  current_size = new_size;
}

void ECExtentCache::Object::unpin(Op &op) {
  op.lines.clear();
  delete_maybe();
}

void ECExtentCache::Object::delete_maybe() const {
  if (lines.empty() && active_ios == 0) {
    pg.objects.erase(oid);
  }
}

void check_seset_empty_for_range(shard_extent_set_t s, uint64_t off, uint64_t len)
{
  for (auto &[shard, eset] : s) {
    ceph_assert(!eset.intersects(off, len));
  }
}

void ECExtentCache::Object::erase_line(uint64_t offset) {
  check_seset_empty_for_range(requesting, offset, line_size);
  do_not_read.erase_stripe(offset, line_size);
  lines.erase(offset);
  delete_maybe();
}

void ECExtentCache::Object::invalidate(OpRef &invalidating_op)
{
  for (auto &[_, l] : lines ) {
    l.lock()->cache->clear();
  }

  /* Remove all entries from the LRU */
  pg.lru.remove_object(oid);

  ceph_assert(!reading);
  do_not_read.clear();
  requesting.clear();
  requesting_ops.clear();
  reading_ops.clear();

  current_size = invalidating_op->projected_size;
  projected_size = current_size;

  // Cache can now be replayed and invalidate teh cache!
  invalidating_op->invalidates_cache = false;

  cache_invalidated = true;
  cache_invalidate_expected = false;

  /* We now need to reply all outstanding ops, so as to regenerate the read */
  for (auto &op : pg.waiting_ops) {
    if (op->object.oid == oid) {
      op->read_done = false;
      request(op);
    }
  }
}

void ECExtentCache::cache_maybe_ready()
{

  while (!waiting_ops.empty()) {
    OpRef op = waiting_ops.front();
    if (op->invalidates_cache) {
      op->object.invalidate(op);
      ceph_assert(!op->invalidates_cache);
    }
    /* If reads_done finds all reads complete it will call the completion
     * callback. Typically, this will cause the client to execute the
     * transaction and pop the front of waiting_ops.  So we abort if either
     * reads are not ready, or the client chooses not to complete the op
     */
    if (!op->complete_if_reads_cached(op))
      return;

    waiting_ops.pop_front();
  }
}

ECExtentCache::OpRef ECExtentCache::prepare(GenContextURef<OpRef &> && ctx,
  hobject_t const &oid,
  std::optional<shard_extent_set_t> const &to_read,
  shard_extent_set_t const &write,
  uint64_t orig_size,
  uint64_t projected_size,
  bool invalidates_cache)
{

  if (!objects.contains(oid)) {
    objects.emplace(oid, Object(*this, oid, orig_size));
  }
  OpRef op = std::make_shared<Op>(
    std::move(ctx), objects.at(oid), to_read, write, projected_size, invalidates_cache);

  return op;
}

void ECExtentCache::read_done(hobject_t const& oid, shard_extent_map_t const&& update)
{
  objects.at(oid).read_done(update);
  cache_maybe_ready();
}

void ECExtentCache::write_done(OpRef const &op, shard_extent_map_t const && update)
{
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

void ECExtentCache::on_change() {
  for (auto && [_, o] : objects) {
    o.reading_ops.clear();
    o.requesting_ops.clear();
    o.requesting.clear();
  }
  for (auto && op : waiting_ops) {
    op->cancel();
  }
  waiting_ops.clear();
}

void ECExtentCache::on_change2()
{
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
  counter++;
  cache_maybe_ready();
}

bool ECExtentCache::idle() const
{
  return active_ios == 0;
}

int ECExtentCache::get_and_reset_counter()
{
  int ret = counter;
  counter = 0;
  return ret;
}

void ECExtentCache::LRU::erase(Key &k)
{
  erase(map.at(k).first);
}

list<ECExtentCache::LRU::Key>::iterator ECExtentCache::LRU::erase(list<Key>::iterator &it)
{
  size -= map.at(*it).second->size();
  map.erase(*it);
  return lru.erase(it);
}

void ECExtentCache::LRU::add(Line &line)
{
  uint64_t _size = line.cache->size();
  if (_size == 0) return;

  const Key k(line.offset, line.object.oid);

  shared_ptr<shard_extent_map_t> cache = line.cache;

  mutex.lock();
  ceph_assert(!map.contains(k));
  auto i = lru.insert(lru.end(), k);
  auto j = make_pair(std::move(i), std::move(cache));
  map.insert(std::pair(std::move(k), std::move(j)));
  size += _size;
  free_maybe();
  mutex.unlock();
}

shared_ptr<shard_extent_map_t> ECExtentCache::LRU::find(hobject_t &oid, uint64_t offset)
{
  Key k(offset, oid);
  shared_ptr<shard_extent_map_t> cache = nullptr;
  mutex.lock();
  if (map.contains(k)) {
    auto &&[lru_iter, c] = map.at(k);
    cache = c;
    erase(lru_iter);
  }
  mutex.unlock();
  return cache;
}

void ECExtentCache::LRU::remove_object(hobject_t &oid)
{
  mutex.lock();
  for (auto it = lru.begin(); it != lru.end(); ) {
    if (it->oid == oid) it = erase(it);
    else ++it;
  }
  mutex.unlock();
}

void ECExtentCache::LRU::free_maybe() {
  while (max_size < size) {
    erase(lru.front());
  }
}

void ECExtentCache::LRU::discard() {
  mutex.lock();
  lru.clear();
  map.clear();
  size = 0;
  mutex.unlock();
}

extent_set ECExtentCache::Op::get_pin_eset(uint64_t alignment) const {
  extent_set eset = writes.get_extent_superset();
  if (reads) reads->get_extent_superset(eset);
  eset.align(alignment);

  return eset;
}

ECExtentCache::Op::Op(GenContextURef<OpRef &> &&cache_ready_cb,
  Object &object,
  std::optional<shard_extent_set_t> const &to_read,
  shard_extent_set_t const &write,
  uint64_t projected_size,
  bool invalidates_cache) :
  object(object),
  reads(to_read),
  writes(write),
  result(&object.sinfo),
  invalidates_cache(invalidates_cache),
  projected_size(projected_size),
  cache_ready_cb(std::move(cache_ready_cb))
{
  object.active_ios++;
  object.pg.active_ios++;
}

shard_extent_map_t ECExtentCache::Object::get_cache(std::optional<shard_extent_set_t> const &set) const
{
  if (!set) return shard_extent_map_t(&sinfo);

  shard_id_map<extent_map> res(sinfo.get_k_plus_m());
  for (auto && [shard, eset] : *set) {
    for ( auto [off, len] : eset) {
      for (uint64_t slice_start = line_align(off);
           slice_start < off + len;
           slice_start += line_size)
      {
        uint64_t offset = max(slice_start, off);
        uint64_t length = min(slice_start + line_size, off  + len) - offset;
        // This line must exist, as it was created when the op was created.
        LineRef l = lines.at(slice_start).lock();
        if (l->cache->contains_shard(shard)) {
          extent_map m = l->cache->get_extent_map(shard).intersect(offset, length);
          if (!m.empty()) {
            if (!res.contains(shard)) res.emplace(shard, std::move(m));
            else res.at(shard).insert(m);
          }
        }
      }
    }
  }
  return shard_extent_map_t(&sinfo, std::move(res));
}
