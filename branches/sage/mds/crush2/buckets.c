
#include "hash.h"
#include "buckets.h"

int 
crush_bucket_uniform_choose(struct crush_bucket_uniform *bucket, int x, int r)
{
  unsigned o, p, s;
  o = crush_hash32_2(x, bucket->h.id);
  p = bucket->primes[crush_hash32_2(bucket->h.id, x) % bucket->h.size];
  s = (x + o + (r+1)*p) % bucket->h.size;
  return bucket->h.items[s];
}

int 
crush_bucket_list_choose(struct crush_bucket_list *bucket, int x, int r)
{
  int i;
  __u64 w;

  for (i=0; i<bucket->h.size; i++) {
    w = crush_hash32_4(x, bucket->h.items[i], r, bucket->h.id) & 0xffff;
    w = (w * bucket->sum_weights[i]) >> 32;
    if (w < bucket->item_weights[i])
      return bucket->h.items[i];
  }

  BUG_ON(1);
  return 0;
}

int 
crush_bucket_tree_choose(struct crush_bucket_tree *bucket, int x, int r)
{
  return 0;
}

int 
crush_bucket_straw_choose(struct crush_bucket_straw *bucket, int x, int r)
{
  int i;
  int high = 0;
  unsigned high_draw = 0;
  __u64 draw;

  for (i=0; i<bucket->h.size; i++) {
    draw = (crush_hash32_3(x, bucket->h.items[i], r) & 0xffff) * bucket->straws[i];
    draw = draw >> 32;
    if (i == 0 || draw > high_draw) {
      high = i;
      high_draw = draw;
    }
  }

  return high;
}
