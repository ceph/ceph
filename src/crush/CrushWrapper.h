// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_WRAPPER_H
#define CEPH_CRUSH_WRAPPER_H

#include <stdlib.h>
#include <map>
#include <set>
#include <string>

#include <iostream> //for testing, remove

#include "include/types.h"

extern "C" {
#include "crush.h"
#include "hash.h"
#include "mapper.h"
#include "builder.h"
}

#include "include/err.h"
#include "include/encoding.h"


#include "common/Mutex.h"

#include "include/assert.h"
#define BUG_ON(x) assert(!(x))

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
  mutable Mutex mapper_lock;
public:
  std::map<int32_t, string> type_map; /* bucket/device type names */
  std::map<int32_t, string> name_map; /* bucket/device names */
  std::map<int32_t, string> rule_name_map;

private:
  struct crush_map *crush;
  /* reverse maps */
  mutable bool have_rmaps;
  mutable std::map<string, int> type_rmap, name_rmap, rule_name_rmap;
  void build_rmaps() const {
    if (have_rmaps) return;
    build_rmap(type_map, type_rmap);
    build_rmap(name_map, name_rmap);
    build_rmap(rule_name_map, rule_name_rmap);
    have_rmaps = true;
  }
  void build_rmap(const map<int, string> &f, std::map<string, int> &r) const {
    r.clear();
    for (std::map<int, string>::const_iterator p = f.begin(); p != f.end(); ++p)
      r[p->second] = p->first;
  }

public:
  CrushWrapper(const CrushWrapper& other);
  const CrushWrapper& operator=(const CrushWrapper& other);

  CrushWrapper() : mapper_lock("CrushWrapper::mapper_lock"),
		   crush(0), have_rmaps(false) {
    create();
  }
  ~CrushWrapper() {
    if (crush)
      crush_destroy(crush);
  }

  crush_map *get_crush_map() { return crush; }

  /* building */
  void create() {
    if (crush)
      crush_destroy(crush);
    crush = crush_create();
    assert(crush);
    have_rmaps = false;

    set_tunables_default();
  }

  // tunables
  void set_tunables_argonaut() {
    crush->choose_local_tries = 2;
    crush->choose_local_fallback_tries = 5;
    crush->choose_total_tries = 19;
    crush->chooseleaf_descend_once = 0;
    crush->chooseleaf_vary_r = 0;
  }
  void set_tunables_bobtail() {
    crush->choose_local_tries = 0;
    crush->choose_local_fallback_tries = 0;
    crush->choose_total_tries = 50;
    crush->chooseleaf_descend_once = 1;
    crush->chooseleaf_vary_r = 0;
  }
  void set_tunables_firefly() {
    crush->choose_local_tries = 0;
    crush->choose_local_fallback_tries = 0;
    crush->choose_total_tries = 50;
    crush->chooseleaf_descend_once = 1;
    crush->chooseleaf_vary_r = 1;
  }

  void set_tunables_legacy() {
    set_tunables_argonaut();
    crush->straw_calc_version = 0;
  }
  void set_tunables_optimal() {
    set_tunables_firefly();
    crush->straw_calc_version = 1;
  }
  void set_tunables_default() {
    set_tunables_bobtail();
    crush->straw_calc_version = 1;
  }

  int get_choose_local_tries() const {
    return crush->choose_local_tries;
  }
  void set_choose_local_tries(int n) {
    crush->choose_local_tries = n;
  }

  int get_choose_local_fallback_tries() const {
    return crush->choose_local_fallback_tries;
  }
  void set_choose_local_fallback_tries(int n) {
    crush->choose_local_fallback_tries = n;
  }

  int get_choose_total_tries() const {
    return crush->choose_total_tries;
  }
  void set_choose_total_tries(int n) {
    crush->choose_total_tries = n;
  }

  int get_chooseleaf_descend_once() const {
    return crush->chooseleaf_descend_once;
  }
  void set_chooseleaf_descend_once(int n) {
    crush->chooseleaf_descend_once = !!n;
  }

  int get_chooseleaf_vary_r() const {
    return crush->chooseleaf_vary_r;
  }
  void set_chooseleaf_vary_r(int n) {
    crush->chooseleaf_vary_r = n;
  }

  int get_straw_calc_version() const {
    return crush->straw_calc_version;
  }
  void set_straw_calc_version(int n) {
    crush->straw_calc_version = n;
  }

  bool has_argonaut_tunables() const {
    return
      crush->choose_local_tries == 2 &&
      crush->choose_local_fallback_tries == 5 &&
      crush->choose_total_tries == 19 &&
      crush->chooseleaf_descend_once == 0 &&
      crush->chooseleaf_vary_r == 0 &&
      crush->straw_calc_version == 0;
  }
  bool has_bobtail_tunables() const {
    return
      crush->choose_local_tries == 0 &&
      crush->choose_local_fallback_tries == 0 &&
      crush->choose_total_tries == 50 &&
      crush->chooseleaf_descend_once == 1 &&
      crush->chooseleaf_vary_r == 0 &&
      crush->straw_calc_version == 0;
  }
  bool has_firefly_tunables() const {
    return
      crush->choose_local_tries == 0 &&
      crush->choose_local_fallback_tries == 0 &&
      crush->choose_total_tries == 50 &&
      crush->chooseleaf_descend_once == 1 &&
      crush->chooseleaf_vary_r == 1 &&
      crush->straw_calc_version == 0;
  }

  bool has_optimal_tunables() const {
    return has_firefly_tunables();
  }
  bool has_legacy_tunables() const {
    return has_argonaut_tunables();
  }

  bool has_nondefault_tunables() const {
    return
      (crush->choose_local_tries != 2 ||
       crush->choose_local_fallback_tries != 5 ||
       crush->choose_total_tries != 19);
  }
  bool has_nondefault_tunables2() const {
    return
      crush->chooseleaf_descend_once != 0;
  }
  bool has_nondefault_tunables3() const {
    return
      crush->chooseleaf_vary_r != 0;
  }
  bool has_v2_rules() const;
  bool has_v3_rules() const;

  bool is_v2_rule(unsigned ruleid) const;
  bool is_v3_rule(unsigned ruleid) const;

  // bucket types
  int get_num_type_names() const {
    return type_map.size();
  }
  int get_type_id(const string& name) const {
    build_rmaps();
    if (type_rmap.count(name))
      return type_rmap[name];
    return -1;
  }
  const char *get_type_name(int t) const {
    std::map<int,string>::const_iterator p = type_map.find(t);
    if (p != type_map.end())
      return p->second.c_str();
    return 0;
  }
  void set_type_name(int i, const string& name) {
    type_map[i] = name;
    if (have_rmaps)
      type_rmap[name] = i;
  }

  // item/bucket names
  bool name_exists(const string& name) const {
    build_rmaps();
    return name_rmap.count(name);
  }
  bool item_exists(int i) {
    return name_map.count(i);
  }
  int get_item_id(const string& name) const {
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
  int set_item_name(int i, const string& name) {
    if (!is_valid_crush_name(name))
      return -EINVAL;
    name_map[i] = name;
    if (have_rmaps)
      name_rmap[name] = i;
    return 0;
  }

  // rule names
  bool rule_exists(string name) const {
    build_rmaps();
    return rule_name_rmap.count(name);
  }
  int get_rule_id(string name) const {
    build_rmaps();
    if (rule_name_rmap.count(name))
      return rule_name_rmap[name];
    return -ENOENT;
  }
  const char *get_rule_name(int t) const {
    std::map<int,string>::const_iterator p = rule_name_map.find(t);
    if (p != rule_name_map.end())
      return p->second.c_str();
    return 0;
  }
  void set_rule_name(int i, const string& name) {
    rule_name_map[i] = name;
    if (have_rmaps)
      rule_name_rmap[name] = i;
  }


  /**
   * find tree nodes referenced by rules by a 'take' command
   *
   * Note that these may not be parentless roots.
   */
  void find_takes(set<int>& roots) const;

  /**
   * find tree roots
   *
   * These are parentless nodes in the map.
   */
  void find_roots(set<int>& roots) const;

  /**
   * see if an item is contained within a subtree
   *
   * @param root haystack
   * @param item needle
   * @return true if the item is located beneath the given node
   */
  bool subtree_contains(int root, int item) const;

private:
  /**
   * search for an item in any bucket
   *
   * @param i item
   * @return true if present
   */
  bool _search_item_exists(int i) const;
public:

  /**
   * see if item is located where we think it is
   *
   * This verifies that the given item is located at a particular
   * location in the hierarchy.  However, that check is imprecise; we
   * are actually verifying that the most specific location key/value
   * is correct.  For example, if loc specifies that rack=foo and
   * host=bar, it will verify that host=bar is correct; any placement
   * above that level in the hierarchy is ignored.  This matches the
   * semantics for insert_item().
   *
   * @param cct cct
   * @param item item id
   * @param loc location to check (map of type to bucket names)
   * @param weight optional pointer to weight of item at that location
   * @return true if item is at specified location
   */
  bool check_item_loc(CephContext *cct, int item, const map<string,string>& loc, int *iweight);
  bool check_item_loc(CephContext *cct, int item, const map<string,string>& loc, float *weight) {
    int iweight;
    bool ret = check_item_loc(cct, item, loc, &iweight);
    if (weight)
      *weight = (float)iweight / (float)0x10000;
    return ret;
  }


  /**
   * returns the (type, name) of the parent bucket of id
   *
   * FIXME: ambiguous for items that occur multiple times in the map
   */
  pair<string,string> get_immediate_parent(int id, int *ret = NULL);
  int get_immediate_parent_id(int id, int *parent);

  /**
   * get the fully qualified location of a device by successively finding
   * parents beginning at ID and ending at highest type number specified in
   * the CRUSH map which assumes that if device foo is under device bar, the
   * type_id of foo < bar where type_id is the integer specified in the CRUSH map
   *
   * returns the location in the form of (type=foo) where type is a type of bucket
   * specified in the CRUSH map and foo is a name specified in the CRUSH map
   */
  map<string, string> get_full_location(int id);

  /*
   * identical to get_full_location(int id) although it returns the type/name
   * pairs in the order they occur in the hierarchy.
   *
   * returns -ENOENT if id is not found.
   */
  int get_full_location_ordered(int id, vector<pair<string, string> >& path);

  /**
   * returns (type_id, type) of all parent buckets between id and
   * default, can be used to check for anomolous CRUSH maps
   */
  map<int, string> get_parent_hierarchy(int id);

  /**
   * enumerate immediate children of given node
   *
   * @param id parent bucket or device id
   * @return number of items, or error
   */
  int get_children(int id, list<int> *children);

  /**
   * insert an item into the map at a specific position
   *
   * Add an item as a specific location of the hierarchy.
   * Specifically, we look for the most specific location constraint
   * for which a bucket already exists, and then create intervening
   * buckets beneath that in order to place the item.
   *
   * Note that any location specifiers *above* the most specific match
   * are ignored.  For example, if we specify that osd.12 goes in
   * host=foo, rack=bar, and row=baz, and rack=bar is the most
   * specific match, we will create host=foo beneath that point and
   * put osd.12 inside it.  However, we will not verify that rack=bar
   * is beneath row=baz or move it.
   *
   * In short, we will build out a hierarchy, and move leaves around,
   * but not adjust the hierarchy's internal structure.  Yet.
   *
   * If the item is already present in the map, we will return EEXIST.
   * If the location key/value pairs are nonsensical
   * (rack=nameofdevice), or location specifies that do not attach us
   * to any existing part of the hierarchy, we will return EINVAL.
   *
   * @param cct cct
   * @param id item id
   * @param weight item weight
   * @param name item name
   * @param loc location (map of type to bucket names)
   * @return 0 for success, negative on error
   */
  int insert_item(CephContext *cct, int id, float weight, string name, const map<string,string>& loc);

  /**
   * move a bucket in the hierarchy to the given location
   *
   * This has the same location and ancestor creation behavior as
   * insert_item(), but will relocate the specified existing bucket.
   *
   * @param cct cct
   * @param id bucket id
   * @param loc location (map of type to bucket names)
   * @return 0 for success, negative on error
   */
  int move_bucket(CephContext *cct, int id, const map<string,string>& loc);

  /**
   * add a link to an existing bucket in the hierarchy to the new location
   *
   * This has the same location and ancestor creation behavior as
   * insert_item(), but will add a new link to the specified existing
   * bucket.
   *
   * @param cct cct
   * @param id bucket id
   * @param loc location (map of type to bucket names)
   * @return 0 for success, negative on error
   */
  int link_bucket(CephContext *cct, int id, const map<string,string>& loc);

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
  int update_item(CephContext *cct, int id, float weight, string name, const map<string,string>& loc);

  /**
   * create or move an item, but do not adjust its weight if it already exists
   *
   * @param cct cct
   * @param item item id
   * @param weight initial item weight (if we need to create it)
   * @param name item name
   * @param loc location (map of type to bucket names)
   * @return 0 for no change, 1 for successful change, negative on error
   */
  int create_or_move_item(CephContext *cct, int item, float weight, string name,
			  const map<string,string>& loc);

  /**
   * remove all instances of an item from the map
   *
   * @param cct cct
   * @param id item id to remove
   * @param unlink_only unlink but do not remove bucket (useful if multiple links or not empty)
   * @return 0 on success, negative on error
   */
  int remove_item(CephContext *cct, int id, bool unlink_only);

  /**
   * remove all instances of an item nested beneath a certain point from the map
   *
   * @param cct cct
   * @param id item id to remove
   * @param ancestor ancestor item id under which to search for id
   * @param unlink_only unlink but do not remove bucket (useful if bucket has multiple links or is not empty)
   * @return 0 on success, negative on error
   */
private:
  bool _maybe_remove_last_instance(CephContext *cct, int id, bool unlink_only);
  int _remove_item_under(CephContext *cct, int id, int ancestor, bool unlink_only);
  bool _bucket_is_in_use(CephContext *cct, int id);
public:
  int remove_item_under(CephContext *cct, int id, int ancestor, bool unlink_only);

  /**
   * calculate the locality/distance from a given id to a crush location map
   *
   * Specifically, we look for the lowest-valued type for which the
   * location of id matches that described in loc.
   *
   * @param cct cct
   * @param id the existing id in the map
   * @param loc a set of key=value pairs describing a location in the hierarchy
   */
  int get_common_ancestor_distance(CephContext *cct, int id,
				   const std::multimap<string,string>& loc);

  /**
   * parse a set of key/value pairs out of a string vector
   *
   * These are used to describe a location in the CRUSH hierarchy.
   *
   * @param args list of strings (each key= or key=value)
   * @param ploc pointer to a resulting location map or multimap
   */
  static int parse_loc_map(const std::vector<string>& args,
			   std::map<string,string> *ploc);
  static int parse_loc_multimap(const std::vector<string>& args,
				std::multimap<string,string> *ploc);

  /**
   * get an item's weight
   *
   * Will return the weight for the first instance it finds.
   *
   * @param cct cct
   * @param id item id to check
   * @return weight of item
   */
  int get_item_weight(int id) const;
  float get_item_weightf(int id) const {
    return (float)get_item_weight(id) / (float)0x10000;
  }
  int get_item_weight_in_loc(int id, const map<string,string> &loc);
  float get_item_weightf_in_loc(int id, const map<string,string> &loc) {
    return (float)get_item_weight_in_loc(id, loc) / (float)0x10000;
  }

  int adjust_item_weight(CephContext *cct, int id, int weight);
  int adjust_item_weightf(CephContext *cct, int id, float weight) {
    return adjust_item_weight(cct, id, (int)(weight * (float)0x10000));
  }
  int adjust_item_weight_in_loc(CephContext *cct, int id, int weight, const map<string,string>& loc);
  int adjust_item_weightf_in_loc(CephContext *cct, int id, float weight, const map<string,string>& loc) {
    return adjust_item_weight_in_loc(cct, id, (int)(weight * (float)0x10000), loc);
  }
  void reweight(CephContext *cct);

  /// check if item id is present in the map hierarchy
  bool check_item_present(int id) const;


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

  /**
   * calculate a map of osds to weights for a given rule
   *
   * Generate a map of which OSDs get how much relative weight for a
   * given rule.
   *
   * @param ruleno [in] rule id
   * @param pmap [out] map of osd to weight
   * @return 0 for success, or negative error code
   */
  int get_rule_weight_osd_map(unsigned ruleno, map<int,float> *pmap);

  /* modifiers */
  int add_rule(int len, int ruleset, int type, int minsize, int maxsize, int ruleno) {
    if (!crush) return -ENOENT;
    crush_rule *n = crush_make_rule(len, ruleset, type, minsize, maxsize);
    assert(n);
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
  int set_rule_step_set_choose_tries(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_SET_CHOOSE_TRIES, val, 0);
  }
  int set_rule_step_set_choose_local_tries(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES, val, 0);
  }
  int set_rule_step_set_choose_local_fallback_tries(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES, val, 0);
  }
  int set_rule_step_set_chooseleaf_tries(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_SET_CHOOSELEAF_TRIES, val, 0);
  }
  int set_rule_step_set_chooseleaf_vary_r(unsigned ruleno, unsigned step, int val) {
    return set_rule_step(ruleno, step, CRUSH_RULE_SET_CHOOSELEAF_VARY_R, val, 0);
  }
  int set_rule_step_choose_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_FIRSTN, val, type);
  }
  int set_rule_step_choose_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSE_INDEP, val, type);
  }
  int set_rule_step_choose_leaf_firstn(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSELEAF_FIRSTN, val, type);
  }
  int set_rule_step_choose_leaf_indep(unsigned ruleno, unsigned step, int val, int type) {
    return set_rule_step(ruleno, step, CRUSH_RULE_CHOOSELEAF_INDEP, val, type);
  }
  int set_rule_step_emit(unsigned ruleno, unsigned step) {
    return set_rule_step(ruleno, step, CRUSH_RULE_EMIT, 0, 0);
  }

  int add_simple_ruleset(string name, string root_name, string failure_domain_type,
			 string mode, int rule_type, ostream *err = 0);

  int remove_rule(int ruleno);


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
  /**
   * detach a bucket from its parent and adjust the parent weight
   *
   * returns the weight of the detached bucket
   **/
  int detach_bucket(CephContext *cct, int item){
    if (!crush)
      return (-EINVAL);

    if (item >= 0)
      return (-EINVAL);

    // check that the bucket that we want to detach exists
    assert( get_bucket(item) );

    // get the bucket's weight
    crush_bucket *b = get_bucket(item);
    unsigned bucket_weight = b->weight;

    // get where the bucket is located
    pair<string, string> bucket_location = get_immediate_parent(item);

    // get the id of the parent bucket
    int parent_id = get_item_id(bucket_location.second);

    // get the parent bucket
    crush_bucket *parent_bucket = get_bucket(parent_id);

    if (!IS_ERR(parent_bucket)) {
      // zero out the bucket weight
      crush_bucket_adjust_item_weight(crush, parent_bucket, item, 0);
      adjust_item_weight(cct, parent_bucket->id, parent_bucket->weight);

      // remove the bucket from the parent
      crush_bucket_remove_item(crush, parent_bucket, item);
    } else if (PTR_ERR(parent_bucket) != -ENOENT) {
      return PTR_ERR(parent_bucket);
    }

    // check that we're happy
    int test_weight = 0;
    map<string,string> test_location;
    test_location[ bucket_location.first ] = (bucket_location.second);

    bool successful_detach = !(check_item_loc(cct, item, test_location, &test_weight));
    assert(successful_detach);
    assert(test_weight == 0);

    return bucket_weight;
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
		 int *items, int *weights, int *idout) {
    if (type == 0)
      return -EINVAL;
    crush_bucket *b = crush_make_bucket(crush, alg, hash, type, size, items, weights);
    assert(b);
    return crush_add_bucket(crush, bucketno, b, idout);
  }
  
  void finalize() {
    assert(crush);
    crush_finalize(crush);
  }

  void start_choose_profile() {
    free(crush->choose_tries);
    crush->choose_tries = (__u32 *)malloc(sizeof(*crush->choose_tries) * crush->choose_total_tries);
    memset(crush->choose_tries, 0,
	   sizeof(*crush->choose_tries) * crush->choose_total_tries);
  }
  void stop_choose_profile() {
    free(crush->choose_tries);
    crush->choose_tries = 0;
  }

  int get_choose_profile(__u32 **vec) {
    if (crush->choose_tries) {
      *vec = crush->choose_tries;
      return crush->choose_total_tries;
    }
    return 0;
  }


  void set_max_devices(int m) {
    crush->max_devices = m;
  }

  int find_rule(int ruleset, int type, int size) const {
    if (!crush) return -1;
    return crush_find_rule(crush, ruleset, type, size);
  }

  bool ruleset_exists(int ruleset) const {
    for (size_t i = 0; i < crush->max_rules; ++i) {
      if (rule_exists(i) && crush->rules[i]->mask.ruleset == ruleset) {
	return true;
      }
    }

    return false;
  }

  /**
   * Return the lowest numbered ruleset of type `type`
   *
   * @returns a ruleset ID, or -1 if no matching rulesets found.
   */
  int find_first_ruleset(int type) const {
    int result = -1;

    for (size_t i = 0; i < crush->max_rules; ++i) {
      if (crush->rules[i]
          && crush->rules[i]->mask.type == type
          && (crush->rules[i]->mask.ruleset < result || result == -1)) {
        result = crush->rules[i]->mask.ruleset;
      }
    }

    return result;
  }

  void do_rule(int rule, int x, vector<int>& out, int maxout,
	       const vector<__u32>& weight) const {
    Mutex::Locker l(mapper_lock);
    int rawout[maxout];
    int scratch[maxout * 3];
    int numrep = crush_do_rule(crush, rule, x, rawout, maxout, &weight[0], weight.size(), scratch);
    if (numrep < 0)
      numrep = 0;
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
  void dump_rules(Formatter *f) const;
  void dump_rule(int ruleset, Formatter *f) const;
  void dump_tunables(Formatter *f) const;
  void list_rules(Formatter *f) const;
  void dump_tree(const vector<__u32>& w, ostream *out, Formatter *f) const;
  static void generate_test_instances(list<CrushWrapper*>& o);

  int get_osd_pool_default_crush_replicated_ruleset(CephContext *cct);

  static bool is_valid_crush_name(const string& s);
  static bool is_valid_crush_loc(CephContext *cct,
				 const map<string,string>& loc);
};
WRITE_CLASS_ENCODER(CrushWrapper)

#endif
