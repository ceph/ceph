/*
 * Argument types used by cls_kvs.cc
 *
 *  Created on: Aug 10, 2012
 *      Author: eleanor
 */

#ifndef CLS_KVS_H_
#define CLS_KVS_H_

#define EBALANCE 137

#include "include/encoding.h"
#include "key_value_store/kv_flat_btree_async.h"

using namespace std;
using ceph::bufferlist;

struct assert_size_args {
  uint64_t bound; //the size to compare to - should be k or 2k
  uint64_t comparator; //should be CEPH_OSD_CMPXATTR_OP_EQ,
		  //CEPH_OSD_CMPXATTR_OP_LT, or
		  //CEPH_OSD_CMPXATTR_OP_GT

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(bound, bl);
    ::encode(comparator, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(bound, p);
    ::decode(comparator, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(assert_size_args)

struct idata_from_key_args {
  string key;
  index_data idata;
  index_data next_idata;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(key, bl);
    ::encode(idata, bl);
    ::encode(next_idata, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(key, p);
    ::decode(idata, p);
    ::decode(next_idata, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(idata_from_key_args)

struct idata_from_idata_args {
  index_data idata;
  index_data next_idata;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(idata, bl);
    ::encode(next_idata, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(idata, p);
    ::decode(next_idata, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(idata_from_idata_args)

struct omap_set_args {
  map<string, bufferlist> omap;
  uint64_t bound;
  bool exclusive;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(omap, bl);
    ::encode(bound, bl);
    ::encode(exclusive, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(omap, p);
    ::decode(bound, p);
    ::decode(exclusive, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(omap_set_args)

struct omap_rm_args {
  std::set<string> omap;
  uint64_t bound;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(omap, bl);
    ::encode(bound, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(omap, p);
    ::decode(bound, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(omap_rm_args)

struct rebalance_args {
  object_data odata;
  uint64_t bound;
  uint64_t comparator;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(odata, bl);
    ::encode(bound, bl);
    ::encode(comparator, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(odata,p);
    ::decode(bound, p);
    ::decode(comparator, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(rebalance_args)


#endif /* CLS_KVS_H_ */
