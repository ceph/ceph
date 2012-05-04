// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_WRAPPER_H
#define CEPH_CRUSH_WRAPPER_H

#define BUG_ON(x) assert(!(x))
#include "include/types.h"

extern "C" {
#include "crush.h"
#include "hash.h"
#include "mapper.h"
#include "builder.h"
}

#include "include/err.h"
#include "include/encoding.h"

#include <stdlib.h>
#include <map>
#include <set>
#include <string>

#include <iostream> //for testing, remove

namespace ceph {
  class Formatter;
}

WRITE_RAW_ENCODER(crush_rule_mask)   // it's all u8's

inline static void encode(const crush_rule_step &s, bufferlist &bl)
{
  ::encode(s.op, bl);
  ::encode(s.arg1, bl);
  ::encode(s.arg2, bl);
}
inline static void decode(crush_rule_step &s, bufferlist::iterator &p)
{
  ::decode(s.op, p);
  ::decode(s.arg1, p);
  ::decode(s.arg2, p);
}

using namespace std;
class CrushWrapper {
public:
  struct crush_map *crush;
  std::map<int, string> type_map; /* bucket/device type names */
  std::map<int, string> name_map; /* bucket/device names */
  std::map<int, string> rule_name_map;

  /* reverse maps */
  mutable bool have_rmaps;
  mutable std::map<string, int> type_rmap, name_rmap, rule_name_rmap;

private:
  void build_rmaps() {
    if (have_rmaps) return;
    build_rmap(type_map, type_rmap);
    build_rmap(name_map, name_rmap);
    build_rmap(rule_name_map, rule_name_rmap);
    have_rmaps = true;
  }
  void build_rmap(const map<int, string> &f, std::map<string, int> &r) {
    r.clear();
    for (std::map<int, string>::const_iterator p = f.begin(); p != f.end(); ++p)
      r[p->second] = p->first;
  }

public:
  CrushWrapper(const CrushWrapper& other);
  const CrushWrapper& operator=(const CrushWrapper& other);

  CrushWrapper() : crush(0), have_rmaps(false) {
    create();
  }
  ~CrushWrapper() {
    if (crush)
      crush_destroy(crush);
  }

  /* building */
  void create() {
    if (crush)
      crush_destroy(crush);
    crush = crush_create();
  }

  // bucket types
  int get_num_type_names() const {
    return type_map.size();
  }
  int get_type_id(const char *s) {
    string name(s);
    build_rmaps();
    if (type_rmap.count(name))
      return type_rmap[name];
    return 0;
  }
  const char *get_type_name(int t) const {
    std::map<int,string>::const_iterator p = type_map.find(t);
    if (p != type_map.end())
      return p->second.c_str();
    return 0;
  }
  void set_type_name(int i, const char *n) {
    string name(n);
    type_map[i] = name;
    if (have_rmaps)
      type_rmap[name] = i;
  }

  // item/bucket names
  bool name_exists(const char *s) {
    string name(s);
    build_rmaps();
    return name_rmap.count(name);
  }
  bool item_exists(int i) {
    return name_map.count(i);
  }
  int get_item_id(const char *s) {
    string name(s);
    build_rmaps();
    if (name_rmap.count(name))
      return name_rmap[name];
    return 0;  /* hrm */
  }
  const char *get_item_name(int t) const {
    std::map<int,string>::const_iterator p = name_map.find(t);
    if (p != name_map.end())
      return p->second.c_str();
    return 0;
  }
  void set_item_name(int i, const char *n) {
    string name(n);
    name_map[i] = name;
    if (have_rmaps)
      name_rmap[name] = i;
  }

  // rule names
  int get_rule_id(const char *n) {
    string name(n);
    build_rmaps();
    if (rule_name_rmap.count(name))
      return rule_name_rmap[name];
    return 0;  /* hrm */
  }
  const char *get_rule_name(int t) const {
    std::map<int,string>::const_iterator p = rule_name_map.find(t);
    if (p != rule_name_map.end())
      return p->second.c_str();
    return 0;
  }
  void set_rule_name(int i, const char *n) {
    string name(n);
    rule_name_map[i] = name;
    if (have_rmaps)
      rule_name_rmap[name] = i;
  }


  void find_roots(set<int>& roots) const;

  /**
   * see if item is located where we think it is
   *
   * @param cct cct
   * @param item item id
   * @param loc location to check (map of type to bucket names)
   * @param weight optional pointer to weight of item at that location
   * @return true if item is at specified location
   */
  bool check_item_loc(CephContext *cct, int item, map<string,string>& loc, int *iweight);
  bool check_item_loc(CephContext *cct, int item, map<string,string>& loc, float *weight) {
    int iweight;
    bool ret = check_item_loc(cct, item, loc, &iweight);
    if (weight)
      *weight = (float)iweight / (float)0x10000;
    return ret;
  }

  /**
   * insert an item into the map at a specific position
   *
   * If the item is already present in the map, we will return EEXIST or similar errors.
   *
   * @param cct cct
   * @param id item id
   * @param weight item weight
   * @param name item name
   * @param loc location (map of type to bucket names)
   * @return 0 for success, negative on error
   */
  int insert_item(CephContext *cct, int id, float weight, string name, map<string,string>& loc);

  /**
   * add or update an item's position in the map
   *
   * This is analogous to insert_item, except we will move an item if
   * it is already present.
   *
   * @param cct cct
   * @param id item id
   * @param weight item weight
   * @param name item name
   * @param loc location (map of type to bucket names)
   * @return 0 for no change, 1 for successful change, negative on error
   */
  int update_item(CephContext *cct, int id, float weight, string name, map<string,string>& loc);

  /**
   * remove an item from the map
   *
   * @param cct cct
   * @param id item id to remove
   * @return 0 on success, negative on error
   */
  int remove_item(CephContext *cct, int id);
  int adjust_item_weight(CephContext *cct, int id, int weight);
  int adjust_item_weightf(CephContext *cct, int id, float weight) {
    return adjust_item_weight(cct, id, (int)(weight * (float)0x10000));
  }
  void reweight(CephContext *cct);

  /*** devices ***/
  int get_max_devices() const {
    if (!crush) return 0;
    return crush->max_devices;
  }


  /*** rules ***/
private:
  crush_rule *get_rule(unsigned ruleno) const {
    if (!crush) return (crush_rule *)(-ENOENT);
    if (ruleno >= crush->max_rules)
      return 0;
    return crush->rules[ruleno];
  }
  crush_rule_step *get_rule_step(unsigned ruleno, unsigned step) const {
    crush_rule *n = get_rule(ruleno);
    if (!n) return (crush_rule_step *)(-EINVAL);
    if (step >= n->len) return (crush_rule_step *)(-EINVAL);
    return &n->steps[step];
  }

public:
  /* accessors */
  int get_max_rules() const {
    if (!crush) return 0;
    return crush->max_rules;
  }
  bool rule_exists(unsigned ruleno) const {
    if (!crush) return false;
    if (ruleno < crush->max_rules &&
	crush->rules[ruleno] != NULL)
      return true;
    return false;
  }
  int get_rule_len(unsigned ruleno) const {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return PTR_ERR(r);
    return r->len;
  }
  int get_rule_mask_ruleset(unsigned ruleno) const {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.ruleset;
  }
  int get_rule_mask_type(unsigned ruleno) const {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.type;
  }
  int get_rule_mask_min_size(unsigned ruleno) const {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.min_size;
  }
  int get_rule_mask_max_size(unsigned ruleno) const {
    crush_rule *r = get_rule(ruleno);
    if (IS_ERR(r)) return -1;
    return r->mask.max_size;
  }
  int get_rule_op(unsigned ruleno, unsigned step) const {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->op;
  }
  int get_rule_arg1(unsigned ruleno, unsigned step) const {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->arg1;
  }
  int get_rule_arg2(unsigned ruleno, unsigned step) const {
    crush_rule_step *s = get_rule_step(ruleno, step);
    if (IS_ERR(s)) return PTR_ERR(s);
    return s->arg2;
  }

  /* modifiers */
  int add_rule(int len, int ruleset, int type, int minsize, int maxsize, int ruleno) {
    if (!crush) return -ENOENT;
    crush_rule *n = crush_make_rule(len, ruleset, type, minsize, maxsize);
    ruleno = crush_add_rule(crush, n, ruleno);
    return ruleno;
  }
  int set_rule_step(unsigned ruleno, unsigned step, int op, int arg1, int arg2) {
    if (!crush) return -ENOENT;
    crush_rule *n = get_rule(ruleno);
    if (!n) return -1;
    crush_rule_set_step(n, step, op, arg1, arg2);
    return 0;
  }
  int set_rule_step_take(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_TAKE, val, 0);
  }
  int set_rule_step_choose_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_FIRSTN, val, type);
  }
  int set_rule_step_choose_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_INDEP, val, type);
  }
  int set_rule_step_choose_leaf_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, val, type);
  }
  int set_rule_step_choose_leaf_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_LEAF_INDEP, val, type);
  }
  int set_rule_step_emit(unsigned ruleno, unsigned step) {
    return set_rule_step(ruleno, step, CRUSH_RULE_EMIT, 0, 0);
  }



  /** buckets **/
private:
  const crush_bucket *get_bucket(int id) const {
    if (!crush)
      return (crush_bucket *)(-EINVAL);
    unsigned int pos = (unsigned int)(-1 - id);
    unsigned int max_buckets = crush->max_buckets;
    if (pos >= max_buckets)
      return (crush_bucket *)(-ENOENT);
    crush_bucket *ret = crush->buckets[pos];
    if (ret == NULL)
      return (crush_bucket *)(-ENOENT);
    return ret;
  }
  crush_bucket *get_bucket(int id) {
    if (!crush)
      return (crush_bucket *)(-EINVAL);
    unsigned int pos = (unsigned int)(-1 - id);
    unsigned int max_buckets = crush->max_buckets;
    if (pos >= max_buckets)
      return (crush_bucket *)(-ENOENT);
    crush_bucket *ret = crush->buckets[pos];
    if (ret == NULL)
      return (crush_bucket *)(-ENOENT);
    return ret;
  }

public:
  int get_max_buckets() const {
    if (!crush) return -EINVAL;
    return crush->max_buckets;
  }
  int get_next_bucket_id() const {
    if (!crush) return -EINVAL;
    return crush_get_next_bucket_id(crush);
  }
  bool bucket_exists(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b))
      return false;
    return true;
  }
  int get_bucket_weight(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->weight;
  }
  float get_bucket_weightf(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return 0;
    return b->weight / (float)0x10000;
  }
  int get_bucket_type(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->type;
  }
  int get_bucket_alg(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->alg;
  }
  int get_bucket_hash(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->hash;
  }
  int get_bucket_size(int id) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return b->size;
  }
  int get_bucket_item(int id, int pos) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    if ((__u32)pos >= b->size)
      return PTR_ERR(b);
    return b->items[pos];
  }
  int get_bucket_item_weight(int id, int pos) const {
    const crush_bucket *b = get_bucket(id);
    if (IS_ERR(b)) return PTR_ERR(b);
    return crush_get_bucket_item_weight(b, pos);
  }

  /* modifiers */
  int add_bucket(int bucketno, int alg, int hash, int type, int size,
		 int *items, int *weights) {
    crush_bucket *b = crush_make_bucket(alg, hash, type, size, items, weights);
    return crush_add_bucket(crush, bucketno, b);
  }
  
  void finalize() {
    assert(crush);
    crush_finalize(crush);
  }

  void set_max_devices(int m) {
    crush->max_devices = m;
  }

  int find_rule(int ruleset, int type, int size) const {
    if (!crush) return -1;
    return crush_find_rule(crush, ruleset, type, size);
  }
  void do_rule(int rule, int x, vector<int>& out, int maxout, int forcefeed,
	       const vector<__u32>& weight) const {
    int rawout[maxout];
    int numrep = crush_do_rule(crush, rule, x, rawout, maxout,
			       forcefeed, &weight[0]);
    if (numrep < 0)
      numrep = 0;   // e.g., when forcefed device dne.
    out.resize(numrep);
    for (int i=0; i<numrep; i++)
      out[i] = rawout[i];
  }

  int read_from_file(const char *fn) {
    bufferlist bl;
    std::string error;
    int r = bl.read_file(fn, &error);
    if (r < 0) return r;
    bufferlist::iterator blp = bl.begin();
    decode(blp);
    return 0;
  }
  int write_to_file(const char *fn) {
    bufferlist bl;
    encode(bl);
    return bl.write_file(fn);
  }

  void encode(bufferlist &bl, bool lean=false) const;
  void decode(bufferlist::iterator &blp);
  void decode_crush_bucket(crush_bucket** bptr, bufferlist::iterator &blp);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<CrushWrapper*>& o);
};
WRITE_CLASS_ENCODER(CrushWrapper)

#endif
