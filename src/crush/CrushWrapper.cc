
#include "common/debug.h"

#include "CrushWrapper.h"

#define DOUT_SUBSYS crush


void CrushWrapper::find_roots(set<int>& roots) const
{
  for (unsigned i=0; i<crush->max_rules; i++) {
    crush_rule *r = crush->rules[i];
    if (!r)
      continue;
    for (unsigned j=0; j<r->len; j++) {
      if (r->steps[j].op == CRUSH_RULE_TAKE)
	roots.insert(r->steps[j].arg1);
    }
  }
}


int CrushWrapper::remove_item(CephContext *cct, int item)
{
  ldout(cct, 5) << "remove_item " << item << dendl;

  crush_bucket *was_bucket = 0;
  int ret = -ENOENT;

  for (int i = 0; i < crush->max_buckets; i++) {
    if (!crush->buckets[i])
      continue;
    crush_bucket *b = crush->buckets[i];

    for (unsigned i=0; i<b->size; ++i) {
      int id = b->items[i];
      if (id == item) {
	if (item < 0) {
	  crush_bucket *t = get_bucket(item);
	  if (t && t->size) {
	    ldout(cct, 1) << "remove_device bucket " << item << " has " << t->size << " items, not empty" << dendl;
	    return -ENOTEMPTY;
	  }	    
	  was_bucket = t;
	}
	ldout(cct, 5) << "remove_device removing item " << item << " from bucket " << b->id << dendl;
	crush_bucket_remove_item(b, item);
	ret = 0;
      }
    }
  }

  if (was_bucket) {
    ldout(cct, 5) << "remove_device removing bucket " << item << dendl;
    crush_remove_bucket(crush, was_bucket);
  }
  if (item >= 0 && name_map.count(item)) {
    name_map.erase(item);
    have_rmaps = false;
    ret = 0;
  }  
  
  return ret;
}

int CrushWrapper::insert_item(CephContext *cct, int item, float weight, string name,
				map<string,string>& loc)  // typename -> bucketname
{
  ldout(cct, 5) << "insert_item item " << item << " weight " << weight
		<< " name " << name << " loc " << loc << dendl;

  if (name_exists(name.c_str())) {
    ldout(cct, 1) << "error: device name '" << name << "' already exists as id "
		  << get_item_id(name.c_str()) << dendl;
    return -EEXIST;
  }

  set_item_name(item, name.c_str());

  int cur = item;

  for (map<int,string>::iterator p = type_map.begin(); p != type_map.end(); p++) {
    if (p->first == 0)
      continue;

    if (loc.count(p->second) == 0) {
      ldout(cct, 1) << "error: did not specify location for '" << p->second << "' level (levels are "
		    << type_map << ")" << dendl;
      return -EINVAL;
    }

    int id = get_item_id(loc[p->second].c_str());
    if (!id) {
      // create the bucket
      ldout(cct, 5) << "insert_item creating bucket " << loc[p->second] << dendl;
      int empty = 0;
      id = add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, p->first, 1, &cur, &empty);
      set_item_name(id, loc[p->second].c_str());
      cur = id;
      continue;
    }

    // add to an existing bucket
    if (!bucket_exists(id)) {
      ldout(cct, 1) << "insert_item don't have bucket " << id << dendl;
      return -EINVAL;
    }

    crush_bucket *b = get_bucket(id);
    assert(b);

    // make sure the item doesn't already exist in this bucket
    for (unsigned j=0; j<b->size; j++)
      if (b->items[j] == cur) {
	ldout(cct, 1) << "insert_item " << cur << " already exists in bucket " << b->id << dendl;
	return -EEXIST;
      }
    
    ldout(cct, 5) << "insert_item adding " << cur << " weight " << weight
		  << " to bucket " << id << dendl;
    crush_bucket_add_item(b, cur, 0);

    // now that we've added the (0-weighted) item and any parent buckets, adjust the weight.
    adjust_item_weightf(cct, item, weight);
    return 0;
  }

  ldout(cct, 1) << "error: didn't find anywhere to add item " << item << " in " << loc << dendl;
  return -EINVAL;
}

int CrushWrapper::adjust_item_weight(CephContext *cct, int id, int weight)
{
  ldout(cct, 5) << "adjust_item_weight " << id << " weight " << weight << dendl;
  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++)
      if (b->items[i] == id) {
	int diff = crush_bucket_adjust_item_weight(b, id, weight);
	ldout(cct, 5) << "adjust_item_weight " << id << " diff " << diff << dendl;
	adjust_item_weight(cct, -1 - bidx, b->weight);
	return 0;
      }
  }
  return -ENOENT;
}

void CrushWrapper::reweight(CephContext *cct)
{
  set<int> roots;
  find_roots(roots);
  for (set<int>::iterator p = roots.begin(); p != roots.end(); p++) {
    if (*p >= 0)
      continue;
    crush_bucket *b = get_bucket(*p);
    ldout(cct, 5) << "reweight bucket " << *p << dendl;
    crush_reweight_bucket(crush, b);
  }
}

void CrushWrapper::encode(bufferlist& bl, bool lean) const
{
  assert(crush);

  __u32 magic = CRUSH_MAGIC;
  ::encode(magic, bl);

  ::encode(crush->max_buckets, bl);
  ::encode(crush->max_rules, bl);
  ::encode(crush->max_devices, bl);

  // buckets
  for (int i=0; i<crush->max_buckets; i++) {
    __u32 alg = 0;
    if (crush->buckets[i]) alg = crush->buckets[i]->alg;
    ::encode(alg, bl);
    if (!alg)
      continue;

    ::encode(crush->buckets[i]->id, bl);
    ::encode(crush->buckets[i]->type, bl);
    ::encode(crush->buckets[i]->alg, bl);
    ::encode(crush->buckets[i]->hash, bl);
    ::encode(crush->buckets[i]->weight, bl);
    ::encode(crush->buckets[i]->size, bl);
    for (unsigned j=0; j<crush->buckets[i]->size; j++)
      ::encode(crush->buckets[i]->items[j], bl);

    switch (crush->buckets[i]->alg) {
    case CRUSH_BUCKET_UNIFORM:
      ::encode(((crush_bucket_uniform*)crush->buckets[i])->item_weight, bl);
      break;

    case CRUSH_BUCKET_LIST:
      for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	::encode(((crush_bucket_list*)crush->buckets[i])->item_weights[j], bl);
	::encode(((crush_bucket_list*)crush->buckets[i])->sum_weights[j], bl);
      }
      break;

    case CRUSH_BUCKET_TREE:
      ::encode(((crush_bucket_tree*)crush->buckets[i])->num_nodes, bl);
      for (unsigned j=0; j<((crush_bucket_tree*)crush->buckets[i])->num_nodes; j++)
	::encode(((crush_bucket_tree*)crush->buckets[i])->node_weights[j], bl);
      break;

    case CRUSH_BUCKET_STRAW:
      for (unsigned j=0; j<crush->buckets[i]->size; j++) {
	::encode(((crush_bucket_straw*)crush->buckets[i])->item_weights[j], bl);
	::encode(((crush_bucket_straw*)crush->buckets[i])->straws[j], bl);
      }
      break;

    default:
      assert(0);
      break;
    }
  }

  // rules
  for (unsigned i=0; i<crush->max_rules; i++) {
    __u32 yes = crush->rules[i] ? 1:0;
    ::encode(yes, bl);
    if (!yes)
      continue;

    ::encode(crush->rules[i]->len, bl);
    ::encode(crush->rules[i]->mask, bl);
    for (unsigned j=0; j<crush->rules[i]->len; j++)
      ::encode(crush->rules[i]->steps[j], bl);
  }

  // name info
  ::encode(type_map, bl);
  ::encode(name_map, bl);
  ::encode(rule_name_map, bl);
}

void CrushWrapper::decode(bufferlist::iterator& blp)
{
  create();

  __u32 magic;
  ::decode(magic, blp);
  if (magic != CRUSH_MAGIC)
    throw buffer::malformed_input("bad magic number");

  ::decode(crush->max_buckets, blp);
  ::decode(crush->max_rules, blp);
  ::decode(crush->max_devices, blp);

  try {
    // buckets
    crush->buckets = (crush_bucket**)calloc(1, crush->max_buckets * sizeof(crush_bucket*));
    for (int i=0; i<crush->max_buckets; i++) {
      decode_crush_bucket(&crush->buckets[i], blp);
    }

    // rules
    crush->rules = (crush_rule**)calloc(1, crush->max_rules * sizeof(crush_rule*));
    for (unsigned i = 0; i < crush->max_rules; ++i) {
      __u32 yes;
      ::decode(yes, blp);
      if (!yes) {
	crush->rules[i] = NULL;
	continue;
      }

      __u32 len;
      ::decode(len, blp);
      crush->rules[i] = (crush_rule*)calloc(1, crush_rule_size(len));
      crush->rules[i]->len = len;
      ::decode(crush->rules[i]->mask, blp);
      for (unsigned j=0; j<crush->rules[i]->len; j++)
	::decode(crush->rules[i]->steps[j], blp);
    }

    // name info
    ::decode(type_map, blp);
    ::decode(name_map, blp);
    ::decode(rule_name_map, blp);
    build_rmaps();

    finalize();
  }
  catch (...) {
    crush_destroy(crush);
    throw;
  }
}

void CrushWrapper::decode_crush_bucket(crush_bucket** bptr, bufferlist::iterator &blp)
{
  __u32 alg;
  ::decode(alg, blp);
  if (!alg) {
    *bptr = NULL;
    return;
  }

  int size = 0;
  switch (alg) {
  case CRUSH_BUCKET_UNIFORM:
    size = sizeof(crush_bucket_uniform);
    break;
  case CRUSH_BUCKET_LIST:
    size = sizeof(crush_bucket_list);
    break;
  case CRUSH_BUCKET_TREE:
    size = sizeof(crush_bucket_tree);
    break;
  case CRUSH_BUCKET_STRAW:
    size = sizeof(crush_bucket_straw);
    break;
  default:
    {
      char str[128];
      snprintf(str, sizeof(str), "unsupported bucket algorithm: %d", alg);
      throw buffer::malformed_input(str);
    }
  }
  crush_bucket *bucket = (crush_bucket*)calloc(1, size);
  *bptr = bucket;
    
  ::decode(bucket->id, blp);
  ::decode(bucket->type, blp);
  ::decode(bucket->alg, blp);
  ::decode(bucket->hash, blp);
  ::decode(bucket->weight, blp);
  ::decode(bucket->size, blp);

  bucket->items = (__s32*)calloc(1, bucket->size * sizeof(__s32));
  for (unsigned j = 0; j < bucket->size; ++j) {
    ::decode(bucket->items[j], blp);
  }

  bucket->perm = (__u32*)calloc(1, bucket->size * sizeof(__s32));
  bucket->perm_n = 0;

  switch (bucket->alg) {
  case CRUSH_BUCKET_UNIFORM:
    ::decode(((crush_bucket_uniform*)bucket)->item_weight, blp);
    break;

  case CRUSH_BUCKET_LIST: {
    crush_bucket_list* cbl = (crush_bucket_list*)bucket;
    cbl->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    cbl->sum_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));

    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(cbl->item_weights[j], blp);
      ::decode(cbl->sum_weights[j], blp);
    }
    break;
  }

  case CRUSH_BUCKET_TREE: {
    crush_bucket_tree* cbt = (crush_bucket_tree*)bucket;
    ::decode(cbt->num_nodes, blp);
    cbt->node_weights = (__u32*)calloc(1, cbt->num_nodes * sizeof(__u32));
    for (unsigned j=0; j<cbt->num_nodes; j++) {
      ::decode(cbt->node_weights[j], blp);
    }
    break;
  }

  case CRUSH_BUCKET_STRAW: {
    crush_bucket_straw* cbs = (crush_bucket_straw*)bucket;
    cbs->straws = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    cbs->item_weights = (__u32*)calloc(1, bucket->size * sizeof(__u32));
    for (unsigned j = 0; j < bucket->size; ++j) {
      ::decode(cbs->item_weights[j], blp);
      ::decode(cbs->straws[j], blp);
    }
    break;
  }

  default:
    // We should have handled this case in the first switch statement
    assert(0);
    break;
  }
}

  

