/*
 * Very simple and fast lockless ring buffer implementatation for
 * one producer and one consumer.
 */

#include <stdint.h>
#include <stddef.h>

/* Do not overcomplicate, choose generic x86 case */
#define L1_CACHE_BYTES 64
#define __cacheline_aligned __attribute__((__aligned__(L1_CACHE_BYTES)))

struct ring_buffer
{
  unsigned int read_idx   __cacheline_aligned;
  unsigned int write_idx  __cacheline_aligned;
  unsigned int size;
  unsigned int low_mask;
  unsigned int high_mask;
  unsigned int bit_shift;
  void         *data_ptr;
};

static inline unsigned int upper_power_of_two(unsigned int v)
{
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v++;

  return v;
}

static inline int ring_buffer_init(struct ring_buffer* rbuf, unsigned int size)
{
  /* Must be pow2 */
  if (((size-1) & size))
    size = upper_power_of_two(size);

  size *= sizeof(void *);
  rbuf->data_ptr = malloc(size);
  rbuf->size = size;
  rbuf->read_idx = 0;
  rbuf->write_idx = 0;
  rbuf->bit_shift = __builtin_ffs(sizeof(void *))-1;
  rbuf->low_mask = rbuf->size - 1;
  rbuf->high_mask = rbuf->size * 2 - 1;

  return 0;
}

static inline void ring_buffer_deinit(struct ring_buffer* rbuf)
{
  free(rbuf->data_ptr);
}

static inline unsigned int ring_buffer_used_size(const struct ring_buffer* rbuf)
{
  __sync_synchronize();
  return ((rbuf->write_idx - rbuf->read_idx) & rbuf->high_mask) >>
    rbuf->bit_shift;
}

static inline void ring_buffer_enqueue(struct ring_buffer* rbuf, void *ptr)
{

  unsigned int idx;

  /*
   * Be aware: we do not check that buffer can be full,
   * assume user of the ring buffer can't submit more.
   */

  idx = rbuf->write_idx & rbuf->low_mask;
  *(void **)((uintptr_t)rbuf->data_ptr + idx) = ptr;
  /* Barrier to be sure stored pointer will be seen properly */
  __sync_synchronize();
  rbuf->write_idx = (rbuf->write_idx + sizeof(ptr)) & rbuf->high_mask;
}

static inline void *ring_buffer_dequeue(struct ring_buffer* rbuf)
{

  unsigned idx;
  void *ptr;

  /*
   * Be aware: we do not check that buffer can be empty,
   * assume user of the ring buffer called ring_buffer_used_size(),
   * which returns actual used size and introduces memory barrier
   * explicitly.
   */

  idx = rbuf->read_idx & rbuf->low_mask;
  ptr = *(void **)((uintptr_t)rbuf->data_ptr + idx);
  rbuf->read_idx = (rbuf->read_idx + sizeof(ptr)) & rbuf->high_mask;

  return ptr;
}
