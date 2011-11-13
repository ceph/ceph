
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
