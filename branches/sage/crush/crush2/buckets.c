
#include "crush.h"
#include "hash.h"

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


static int height(int n) {
	int h = 0;
	while ((n & 1) == 0) {
		h++; 
		n = n >> 1;
	}
	return h;
}
static int left(int x) {
	int h = height(x);
	return x - (1 << (h-1));
}
static int right(int x) {
	int h = height(x);
	return x + (1 << (h-1));
}
static int terminal(int x) {
	return x & 1;
}

int 
crush_bucket_tree_choose(struct crush_bucket_tree *bucket, int x, int r)
{
	int n, l;
	__u32 w;
	__u64 t;

	/* start at root */
	n = bucket->h.size >> 1;

	while (!terminal(n)) {
		/* pick point in [0, w) */
		w = bucket->node_weights[n];
		t = (__u64)crush_hash32_4(x, n, r, bucket->h.id) * (__u64)w;
		t = t >> 32;
		
		/* left or right? */
		l = left(n);
		if (t < bucket->node_weights[l])
			n = l;
		else
			n = right(n);
	}

	return bucket->h.items[n];
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



void crush_destroy_bucket_uniform(struct crush_bucket_uniform *b)
{
	free(b->primes);
	free(b->h.items);
}

void crush_destroy_bucket_list(struct crush_bucket_list *b)
{
	free(b->item_weights);
	free(b->sum_weights);
	free(b->h.items);
}

void crush_destroy_bucket_tree(struct crush_bucket_tree *b)
{
	free(b->node_weights);
}

void crush_destroy_bucket_straw(struct crush_bucket_straw *b)
{
	free(b->straws);
	free(b->h.items);
}
