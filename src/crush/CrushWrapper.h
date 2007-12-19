// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef __CRUSH_WRAPPER_H
#define __CRUSH_WRAPPER_H

#include "crush.h"
#include "hash.h"
#include "mapper.h"
#include "builder.h"

#include "include/encodable.h"

#include <map>
#include <set>

class CrushWrapper {
public:
  struct crush_map *map;
  
  CrushWrapper() : map(0) {}
  ~CrushWrapper() {
    if (map) crush_destroy(map);
  }  

  void create() {
    if (map) crush_destroy(map);
    map = crush_create();
  }
  void finalize() {
    assert(map);
    crush_finalize(map);
  }

  void set_offload(int i, unsigned o) {
    assert(i < map->max_devices);
    map->device_offload[i] = o;
  }
  unsigned get_offload(int i) {
    assert(i < map->max_devices);
    return map->device_offload[i];
  }

  void do_rule(int rule, int x, vector<int>& out, int maxout, int forcefeed) {
    int rawout[maxout];
    
    int numrep = crush_do_rule(map, rule, x, rawout, maxout, forcefeed);

    out.resize(numrep);
    for (int i=0; i<numrep; i++)
      out[i] = rawout[i];
  }

  void _encode(bufferlist &bl) {
    ::_encode_simple(map->max_buckets, bl);
    ::_encode_simple(map->max_rules, bl);
    ::_encode_simple(map->max_devices, bl);

    // simple arrays
    bl.append((char*)map->device_offload, sizeof(map->device_offload[0]) * map->max_devices);

    // buckets
    for (unsigned i=0; i<map->max_buckets; i++) {
      __u32 type = 0;
      if (map->buckets[i]) type = map->buckets[i]->bucket_type;
      ::_encode_simple(type, bl);
      if (!type) continue;

      ::_encode_simple(map->buckets[i]->id, bl);
      ::_encode_simple(map->buckets[i]->type, bl);
      ::_encode_simple(map->buckets[i]->bucket_type, bl);
      ::_encode_simple(map->buckets[i]->weight, bl);
      ::_encode_simple(map->buckets[i]->size, bl);
      for (unsigned j=0; j<map->buckets[i]->size; j++)
	::_encode_simple(map->buckets[i]->items[j], bl);
      
      switch (map->buckets[i]->type) {
      case CRUSH_BUCKET_UNIFORM:
	for (unsigned j=0; j<map->buckets[i]->size; j++)
	  ::_encode_simple(((crush_bucket_uniform*)map->buckets[i])->primes[j], bl);
	::_encode_simple(((crush_bucket_uniform*)map->buckets[i])->item_weight, bl);
	break;

      case CRUSH_BUCKET_LIST:
	for (unsigned j=0; j<map->buckets[i]->size; j++) {
	  ::_encode_simple(((crush_bucket_list*)map->buckets[i])->item_weights[j], bl);
	  ::_encode_simple(((crush_bucket_list*)map->buckets[i])->sum_weights[j], bl);
	}
	break;

      case CRUSH_BUCKET_TREE:
	for (unsigned j=0; j<map->buckets[i]->size; j++) 
	  ::_encode_simple(((crush_bucket_tree*)map->buckets[i])->node_weights[j], bl);
	break;

      case CRUSH_BUCKET_STRAW:
	for (unsigned j=0; j<map->buckets[i]->size; j++) 
	  ::_encode_simple(((crush_bucket_straw*)map->buckets[i])->straws[j], bl);
	break;
      }
    }

    // rules
    for (unsigned i=0; i<map->max_rules; i++) {
      __u32 yes = map->rules[i] ? 1:0;
      ::_encode_simple(yes, bl);
      if (!yes) continue;

      ::_encode_simple(map->rules[i]->len, bl);
      for (unsigned j=0; j<map->rules[i]->len; j++)
	::_encode_simple(map->rules[i]->steps[j], bl);
    }
  }

  void _decode(bufferlist::iterator &blp) {
    create();
    ::_decode_simple(map->max_buckets, blp);
    ::_decode_simple(map->max_rules, blp);
    ::_decode_simple(map->max_devices, blp);

    map->device_offload = (__u32*)malloc(sizeof(map->device_offload[0])*map->max_devices);
    blp.copy(sizeof(map->device_offload[0])*map->max_devices, (char*)map->device_offload);
    
    // buckets
    map->buckets = (crush_bucket**)malloc(sizeof(crush_bucket*)*map->max_buckets);
    for (unsigned i=0; i<map->max_buckets; i++) {
      __u32 type;
      ::_decode_simple(type, blp);
      if (!type) {
	map->buckets[i] = 0;
	continue;
      }

      int size = 0;
      switch (type) {
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
	assert(0);
      }
      map->buckets[i] = (crush_bucket*)malloc(size);
      memset(map->buckets[i], 0, size);
      
      ::_decode_simple(map->buckets[i]->id, blp);
      ::_decode_simple(map->buckets[i]->type, blp);
      ::_decode_simple(map->buckets[i]->bucket_type, blp);
      ::_decode_simple(map->buckets[i]->weight, blp);
      ::_decode_simple(map->buckets[i]->size, blp);

      map->buckets[i]->items = (__s32*)malloc(sizeof(__s32)*map->buckets[i]->size);
      for (unsigned j=0; j<map->buckets[i]->size; j++)
	::_decode_simple(map->buckets[i]->items[j], blp);

      switch (map->buckets[i]->type) {
      case CRUSH_BUCKET_UNIFORM:
	((crush_bucket_uniform*)map->buckets[i])->primes = 
	  (__u32*)malloc(map->buckets[i]->size * sizeof(__u32));
	for (unsigned j=0; j<map->buckets[i]->size; j++)
	  ::_decode_simple(((crush_bucket_uniform*)map->buckets[i])->primes[j], blp);
	::_decode_simple(((crush_bucket_uniform*)map->buckets[i])->item_weight, blp);
	break;

      case CRUSH_BUCKET_LIST:
	((crush_bucket_list*)map->buckets[i])->item_weights = 
	  (__u32*)malloc(map->buckets[i]->size * sizeof(__u32));
	((crush_bucket_list*)map->buckets[i])->sum_weights = 
	  (__u32*)malloc(map->buckets[i]->size * sizeof(__u32));

	for (unsigned j=0; j<map->buckets[i]->size; j++) {
	  ::_decode_simple(((crush_bucket_list*)map->buckets[i])->item_weights[j], blp);
	  ::_decode_simple(((crush_bucket_list*)map->buckets[i])->sum_weights[j], blp);
	}
	break;

      case CRUSH_BUCKET_TREE:
	((crush_bucket_tree*)map->buckets[i])->node_weights = 
	  (__u32*)malloc(map->buckets[i]->size * sizeof(__u32));
	for (unsigned j=0; j<map->buckets[i]->size; j++) 
	  ::_decode_simple(((crush_bucket_tree*)map->buckets[i])->node_weights[j], blp);
	break;

      case CRUSH_BUCKET_STRAW:
	((crush_bucket_straw*)map->buckets[i])->straws = 
	  (__u32*)malloc(map->buckets[i]->size * sizeof(__u32));
	for (unsigned j=0; j<map->buckets[i]->size; j++) 
	  ::_decode_simple(((crush_bucket_straw*)map->buckets[i])->straws[j], blp);
	break;
      }
    }

    // rules
    map->rules = (crush_rule**)malloc(sizeof(crush_rule*)*map->max_rules);
    for (unsigned i=0; i<map->max_rules; i++) {
      __u32 yes;
      ::_decode_simple(yes, blp);
      if (!yes) {
	map->rules[i] = 0;
	continue;
      }

      __u32 len;
      ::_decode_simple(len, blp);
      map->rules[i] = (crush_rule*)malloc(crush_rule_size(len));
      map->rules[i]->len = len;
      for (unsigned j=0; j<map->rules[i]->len; j++)
	::_decode_simple(map->rules[i]->steps[j], blp);
    }

    finalize();
  }
};

#endif
