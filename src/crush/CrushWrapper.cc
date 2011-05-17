
#include "common/debug.h"

#include "CrushWrapper.h"

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


int CrushWrapper::insert_device(int item, int weight, string name,
				map<string,string>& loc)  // typename -> bucketname
{
  cout << "insert_device item " << item << " weight " << weight
	  << " name " << name << " loc " << loc << std::endl;

  if (name_exists(name.c_str())) {
    cerr << "error: device name '" << name << "' already exists" << std::endl;
    return -EEXIST;
  }
  if (item_exists(item)) {
    cerr << "error: device id '" << item << "' already exists" << std::endl;
    return -EEXIST;
  }

  set_item_name(item, name.c_str());

  for (map<int,string>::iterator p = type_map.begin(); p != type_map.end(); p++) {
    if (p->first == 0)
      continue;

    if (loc.count(p->second) == 0) {
      cerr << "error: did not specify location for '" << p->second << "' level (levels are "
	   << type_map << ")" << std::endl;
      return -EINVAL;
    }

    int id = get_item_id(loc[p->second].c_str());
    if (!id) {
      // create the bucket
      cout << "insert_device creating bucket " << loc[p->second] << std::endl;
      int empty = 0;
      id = add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, p->first, 1, &item, &empty);
      set_item_name(id, loc[p->second].c_str());
      item = id;
      continue;
    }

    // add to an existing bucket
    if (!bucket_exists(id)) {
      cout << "insert_device don't have bucket " << id << std::endl;
      return -EINVAL;
    }
      
    cout << "insert_device adding " << item << " weight " << weight
	    << " to bucket " << id << std::endl;
    crush_bucket *b = (crush_bucket *)get_bucket(id);
    assert(b);
    crush_bucket_add_item(b, item, 0);
    adjust_item_weight(item, weight);
    return 0;
  }

  cerr << "error: didn't find anywhere to add item " << item << " in " << loc << std::endl;
  return -EINVAL;
}

void CrushWrapper::adjust_item_weight(int id, int weight)
{
  cout << "adjust_item_weight " << id << " weight " << weight << std::endl;
  for (int bidx = 0; bidx < crush->max_buckets; bidx++) {
    crush_bucket *b = crush->buckets[bidx];
    if (b == 0)
      continue;
    for (unsigned i = 0; i < b->size; i++)
      if (b->items[i] == id) {
	int diff = crush_bucket_adjust_item_weight(b, id, weight);
	cout << "adjust_item_weight " << id << " diff " << diff << std::endl;
	adjust_item_weight(-1 - bidx, b->weight);
      }
  }
}

void CrushWrapper::reweight()
{
  set<int> roots;
  find_roots(roots);
  for (set<int>::iterator p = roots.begin(); p != roots.end(); p++) {
    if (*p >= 0)
      continue;
    crush_bucket *b = (struct crush_bucket *)get_bucket(*p);
    cout << "reweight bucket " << *p << std::endl;
    crush_reweight_bucket(crush, b);
  }
}
