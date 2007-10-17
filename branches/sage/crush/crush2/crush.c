
#include "crush.h"
#include "hash.h"

/*
 * choose numrep distinct items of given type
 */
static int crush_choose(struct crush_map *map,
			struct crush_bucket *bucket,
			int x, int numrep, int type,
			int *out, int firstn)
{
	int rep;
	int ftotal, flocal;
	int retry_rep, skip_rep;
	struct crush_bucket *in = bucket;
	int r;
	int i;
	int item;
	int itemtype;
	int outpos;
	int collide, bad;
	
	outpos = 0;
	
	for (rep = 0; rep < numrep; rep++) {
		/* keep trying until we get a non-out, non-colliding item */
		ftotal = 0;
		skip_rep = 0;
		
		while (1) {
			in = bucket;               /* initial bucket */
			
			/* choose through intervening buckets */
			flocal = 0;
			retry_rep = 0;
			
			while (1) {
				r = rep;
				if (in->type == CRUSH_BUCKET_UNIFORM) {
					/* be careful */
					if (firstn || numrep >= in->size) {
						r += ftotal;           /* r' = r + f_total */
					} else {
						r += numrep * flocal;  /* r' = r + n*f_local */
						/* make sure numrep is not a multiple of bucket size */
						if (in->size % numrep == 0)
							/* shift seq once per pass through the bucket */
							r += numrep * flocal / in->size;  
					}
				} else {
					if (firstn) 
						r += ftotal;           /* r' = r + f_total */
					else 
						r += numrep * flocal;  /* r' = r + n*f_local */
				}

				/* bucket choose */
				switch (in->type) {
				case CRUSH_BUCKET_UNIFORM:
					item = crush_bucket_uniform_choose((struct crush_bucket_uniform*)in, x, r);
					break;
				case CRUSH_BUCKET_LIST:
					item = crush_bucket_list_choose((struct crush_bucket_list*)in, x, r);
					break;
				case CRUSH_BUCKET_TREE:
					item = crush_bucket_tree_choose((struct crush_bucket_tree*)in, x, r);
					break;
				case CRUSH_BUCKET_STRAW:
					item = crush_bucket_straw_choose((struct crush_bucket_straw*)in, x, r);
					break;
				default:
					BUG_ON(1);
				}
				
				/* desired type? */
				if (item < 0) 
					itemtype = map->buckets[-item]->type;
				else 
					itemtype = 0;
				
				/* keep going? */
				if (itemtype != type) {
					in = map->buckets[-item];
					continue;
				}
				
				/* collision? */
				collide = 0;
				for (i=0; i<rep; i++) {
					if (out[i] == item) {
						collide = 1;
						break;
					}
				}
				
				/* bad (out)? */
				bad = 0;
				if (itemtype == 0 && map->device_offload[item]) {
					if (map->device_offload[item] >= 0x10000) 
						bad = 1;
					else if ((crush_hash32_2(x, item) & 0xffff) < map->device_offload[item])
						bad = 1;
				}
				
				if (bad || collide) {
					ftotal++;
					flocal++;
					
					if (collide && flocal < 3) 
						continue;   /* locally a few times */
					if (ftotal >= 10) {
						/* give up, ignore dup, fixme */
						skip_rep = 1;
						break;
					}
					retry_rep = 1;
				}
				break;
			}
			
			if (retry_rep) continue;
		}
		
		if (skip_rep) continue;
		
		out[outpos] = item;
		outpos++;
	}
	
	return outpos;
}


int crush_do_rule(struct crush_map *map,
		  int ruleno,
		  int x, int *result, int result_max,
		  int forcefeed)    /* -1 for none */
{
	int result_len;
	int force_stack[CRUSH_MAX_DEPTH];
	int force_pos = -1;
	int a[CRUSH_MAX_SET];
	int b[CRUSH_MAX_SET];
	int *w;
	int wsize = 0;
	int *o;
	int osize;
	int *tmp;
	struct crush_rule *rule;
	int step;
	int i;
	int numrep;
	
	rule = map->rules[ruleno];
	result_len = 0;
	w = a;
	o = b;
	
	/* determine hierarchical context of forcefeed, if any */
	if (forcefeed >= 0) {
		while (1) {
			force_stack[++force_pos] = forcefeed;
			if (forcefeed >= 0)
				forcefeed = map->device_parents[forcefeed];
			else
				forcefeed = map->bucket_parents[-forcefeed];
			if (forcefeed == 0) break;
		}
	}
	
	for (step = 0; step < rule->len; step++) {
		switch (rule->steps[step].op) {
		case CRUSH_RULE_TAKE:
			if (force_pos >= 0) {
				w[0] = force_stack[force_pos];
				force_pos--;
				BUG_ON(w[0] != rule->steps[step].arg1);
			} else {
				w[0] = rule->steps[step].arg1;
			}
			wsize = 1;
			break;
			
		case CRUSH_RULE_CHOOSE_FIRSTN:
		case CRUSH_RULE_CHOOSE_INDEP:
			BUG_ON(wsize == 0);
			
			/* reset output */
			osize = 0;
			
			for (i = 0; i < wsize; i++) {
				numrep = rule->steps[step].arg1;
				
				if (force_pos >= 0) {
					o[osize++] = force_stack[force_pos];
					force_pos--;
					numrep--;
				}
				if (numrep)
					crush_choose(map,
						     map->buckets[-w[i]],
						     x, numrep, rule->steps[step].arg2,
						     o+osize, rule->steps[step].op == CRUSH_RULE_CHOOSE_FIRSTN);
			}
			
			/* swap t and w arrays */
			tmp = o;
			o = w;
			w = o;
			wsize = osize;
			break;      
			
			
		case CRUSH_RULE_EMIT:
			for (i=0; i<wsize && result_max; i++) {
				result[result_len] = w[i];
				result_len++;
				result_max--;
			}
			wsize = 0;
			break;
			
		default:
			BUG_ON(1);
		}
	}
	
	return result_len;
}

