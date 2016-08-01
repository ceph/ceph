/*
 * Uses a two-level B-tree to store a set of key-value pairs.
 *
 * September 2, 2012
 * Eleanor Cawthon
 * eleanor.cawthon@inktank.com
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef KVFLATBTREEASYNC_H_
#define KVFLATBTREEASYNC_H_

#define ESUICIDE 134
#define EPREFIX 136
#define EFIRSTOBJ 138

#include "key_value_store/key_value_structure.h"
#include "include/utime.h"
#include "include/types.h"
#include "include/encoding.h"
#include "common/Mutex.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "global/global_context.h"
#include "include/rados/librados.hpp"
#include <cfloat>
#include <queue>
#include <sstream>
#include <stdarg.h>

using namespace std;
using ceph::bufferlist;

enum {
  ADD_PREFIX = 1,
  MAKE_OBJECT = 2,
  UNWRITE_OBJECT = 3,
  RESTORE_OBJECT = 4,
  REMOVE_OBJECT = 5,
  REMOVE_PREFIX = 6,
  AIO_MAKE_OBJECT = 7
};

struct rebalance_args;


/**
 * stores information about a key in the index.
 *
 * prefix is "0" unless key is "", in which case it is "1". This ensures that
 * the object with key "" will always be the highest key in the index.
 */
struct key_data {
  string raw_key;
  string prefix;

  key_data()
  {}

  /**
   * @pre: key is a raw key (does not contain a prefix)
   */
  key_data(string key)
  : raw_key(key)
  {
    raw_key == "" ? prefix = "1" : prefix = "0";
  }

  bool operator==(key_data k) const {
    return ((raw_key == k.raw_key) && (prefix == k.prefix));
  }

  bool operator!=(key_data k) const {
    return ((raw_key != k.raw_key) || (prefix != k.prefix));
  }

  bool operator<(key_data k) const {
    return this->encoded() < k.encoded();
  }

  bool operator>(key_data k) const {
    return this->encoded() > k.encoded();
  }

  /**
   * parses the prefix from encoded and stores the data in this.
   *
   * @pre: encoded has a prefix
   */
  void parse(string encoded) {
    prefix = encoded[0];
    raw_key = encoded.substr(1,encoded.length());
  }

  /**
   * returns a string containing the encoded (prefixed) key
   */
  string encoded() const {
    return prefix + raw_key;
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(raw_key, bl);
    ::encode(prefix, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(raw_key, p);
    ::decode(prefix, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(key_data)


/**
 * Stores information read from a librados object.
 */
struct object_data {
  key_data min_kdata; //the max key from the previous index entry
  key_data max_kdata; //the max key, from the index
  string name; //the object's name
  map<std::string, bufferlist> omap; // the omap of the object
  bool unwritable; // an xattr that, if false, means an op is in
		  // progress and other clients should not write to it.
  uint64_t version; //the version at time of read
  uint64_t size; //the number of elements in the omap

  object_data() 
  : unwritable(false),
    version(0),
    size(0) 
  {}

  object_data(string the_name)
  : name(the_name),
    unwritable(false),
    version(0),
    size(0) 
  {}

  object_data(key_data min, key_data kdat, string the_name)
  : min_kdata(min),
    max_kdata(kdat),
    name(the_name),
    unwritable(false),
    version(0),
    size(0) 
  {}

  object_data(key_data min, key_data kdat, string the_name,
      map<std::string, bufferlist> the_omap)
  : min_kdata(min),
    max_kdata(kdat),
    name(the_name),
    omap(the_omap),
    unwritable(false),
    version(0),
    size(0) 
  {}

  object_data(key_data min, key_data kdat, string the_name, int the_version)
  : min_kdata(min),
    max_kdata(kdat),
    name(the_name),
    unwritable(false),
    version(the_version),
    size(0) 
  {}

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(min_kdata, bl);
    ::encode(max_kdata, bl);
    ::encode(name, bl);
    ::encode(omap, bl);
    ::encode(unwritable, bl);
    ::encode(version, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(min_kdata, p);
    ::decode(max_kdata, p);
    ::decode(name, p);
    ::decode(omap, p);
    ::decode(unwritable, p);
    ::decode(version, p);
    ::decode(size, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(object_data)

/**
 * information about objects to be created by a split or merge - stored in the
 * index_data.
 */
struct create_data {
  key_data min;
  key_data max;
  string obj;

  create_data()
  {}

  create_data(key_data n, key_data x, string o)
  : min(n),
    max(x),
    obj(o)
  {}

  create_data(object_data o)
  : min(o.min_kdata),
    max(o.max_kdata),
    obj(o.name)
  {}

  create_data & operator=(const create_data &c) {
    min = c.min;
    max = c.max;
    obj = c.obj;
    return *this;
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(min, bl);
    ::encode(max, bl);
    ::encode(obj, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(min, p);
    ::decode(max, p);
    ::decode(obj, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(create_data)

/**
 * information about objects to be deleted by a split or merge - stored in the
 * index_data.
 */
struct delete_data {
  key_data min;
  key_data max;
  string obj;
  uint64_t version;

  delete_data()
  : version(0)
  {}

  delete_data(key_data n, key_data x, string o, uint64_t v)
  : min(n),
    max(x),
    obj(o),
    version(v)
  {}

  delete_data & operator=(const delete_data &d) {
    min = d.min;
    max = d.max;
    obj = d.obj;
    version = d.version;
    return *this;
  }


  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(min, bl);
    ::encode(max, bl);
    ::encode(obj, bl);
    ::encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(min, p);
    ::decode(max, p);
    ::decode(obj, p);
    ::decode(version, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(delete_data)

/**
 * The index object is a key value map that stores
 * the highest key stored in an object as keys, and an index_data
 * as the corresponding value. The index_data contains the encoded
 * high and low keys (where keys in this object are > min_kdata and
 *  <= kdata), the name of the librados object where keys containing
 * that range of keys are located, and information about split and
 * merge operations that may need to be cleaned up if a client dies.
 */
struct index_data {
  //the encoded key corresponding to the object
  key_data kdata;

  //"1" if there is a prefix (because a split or merge is
  //in progress), otherwise ""
  string prefix;

  //the kdata of the previous index entry
  key_data min_kdata;

  utime_t ts; //time that a split/merge started

  //objects to be created
  vector<create_data > to_create;

  //objects to be deleted
  vector<delete_data > to_delete;

  //the name of the object where the key range is located.
  string obj;

  index_data()
  {}

  index_data(string raw_key)
  : kdata(raw_key)
  {}

  index_data(key_data max, key_data min, string o)
  : kdata(max),
    min_kdata(min),
    obj(o)
  {}

  index_data(create_data c)
  : kdata(c.max),
    min_kdata(c.min),
    obj(c.obj)
  {}

  bool operator<(const index_data &other) const {
    return (kdata.encoded() < other.kdata.encoded());
  }

  //true if there is a prefix and now - ts > timeout.
  bool is_timed_out(utime_t now, utime_t timeout) const;

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(prefix, bl);
    ::encode(min_kdata, bl);
    ::encode(kdata, bl);
    ::encode(ts, bl);
    ::encode(to_create, bl);
    ::encode(to_delete, bl);
    ::encode(obj, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(prefix, p);
    ::decode(min_kdata, p);
    ::decode(kdata, p);
    ::decode(ts, p);
    ::decode(to_create, p);
    ::decode(to_delete, p);
    ::decode(obj, p);
    DECODE_FINISH(p);
  }

  /*
   * Prints a string representation of the information, in the following format:
   * (min_kdata/
   * kdata,
   * prefix
   * ts
   * elements of to_create, organized into (high key| obj name)
   * ;
   * elements of to_delete, organized into (high key| obj name | version number)
   * :
   * val)
   */
  string str() const {
    stringstream strm;
    strm << '(' << min_kdata.encoded() << "/" << kdata.encoded() << ','
	<< prefix;
    if (prefix == "1") {
      strm << ts.sec() << '.' << ts.usec();
      for(vector<create_data>::const_iterator it = to_create.begin();
	  it != to_create.end(); ++it) {
	  strm << '(' << it->min.encoded() << '/' << it->max.encoded() << '|'
	      << it->obj << ')';
      }
      strm << ';';
      for(vector<delete_data >::const_iterator it = to_delete.begin();
	  it != to_delete.end(); ++it) {
	  strm << '(' << it->min.encoded() << '/' << it->max.encoded() << '|'
	      << it->obj << '|'
	      << it->version << ')';
      }
      strm << ':';
    }
    strm << obj << ')';
    return strm.str();
  }
};
WRITE_CLASS_ENCODER(index_data)

/**
 * Structure to store information read from the index for reuse.
 */
class IndexCache {
protected:
  map<key_data, pair<index_data, utime_t> > k2itmap;
  map<utime_t, key_data> t2kmap;
  int cache_size;

public:
  IndexCache(int n)
  : cache_size(n)
  {}
  /**
   * Inserts idata into the cache and removes whatever key mapped to before.
   * If the cache is full, pops the oldest entry.
   */
  void push(const string &key, const index_data &idata);

  /**
   * Inserts idata into the cache. If idata.kdata is already in the cache,
   * replaces the old one. Pops the oldest entry if the cache is full.
   */
  void push(const index_data &idata);

  /**
   * Removes the oldest entry from the cache
   */
  void pop();

  /**
   * Removes the value associated with kdata from both maps
   */
  void erase(key_data kdata);

  /**
   * gets the idata where key belongs. If none, returns -ENODATA.
   */
  int get(const string &key, index_data *idata) const;

  /**
   * Gets the idata where key goes and the one after it. If there are not
   * valid entries for both of them, returns -ENODATA.
   */
  int get(const string &key, index_data *idata, index_data * next_idata) const;
  void clear();
};

class KvFlatBtreeAsync;


/**
 * These are used internally to translate aio operations into useful thread
 * arguments.
 */
struct aio_set_args {
  KvFlatBtreeAsync * kvba;
  string key;
  bufferlist val;
  bool exc;
  callback cb;
  void * cb_args;
  int * err;
};

struct aio_rm_args {
  KvFlatBtreeAsync * kvba;
  string key;
  callback cb;
  void * cb_args;
  int * err;
};

struct aio_get_args {
  KvFlatBtreeAsync * kvba;
  string key;
  bufferlist * val;
  bool exc;
  callback cb;
  void * cb_args;
  int * err;
};

class KvFlatBtreeAsync : public KeyValueStructure {
protected:

  //don't change these once operations start being called - they are not
  //protected with mutexes!
  int k;
  string index_name;
  librados::IoCtx io_ctx;
  string rados_id;
  string client_name;
  librados::Rados rados;
  string pool_name;
  injection_t interrupt;
  int wait_ms;
  utime_t timeout; //declare a client dead if it goes this long without
		   //finishing a split/merge
  int cache_size;
  double cache_refresh; //read cache_size / cache_refresh entries each time the
			//index is read
  bool verbose;//if true, display lots of debug output

  //shared variables protected with mutexes
  Mutex client_index_lock;
  int client_index; //names of new objects are client_name.client_index
  Mutex icache_lock;
  IndexCache icache;
  friend struct index_data;

  /**
   * finds the object in the index with the lowest key value that is greater
   * than idata.kdata. If idata.kdata is the max key, returns -EOVERFLOW. If
   * idata has a prefix and has timed out, cleans up.
   *
   * @param idata: idata for the object to search for.
   * @param out_data: the idata for the next object.
   *
   * @pre: idata must contain a key_data.
   * @post: out_data contains complete information
   */
  int next(const index_data &idata, index_data * out_data);

  /**
   * finds the object in the index with the lowest key value that is greater
   * than idata.kdata. If idata.kdata is the lowest key, returns -ERANGE If
   * idata has a prefix and has timed out, cleans up.
   *
   * @param idata: idata for the object to search for.
   * @param out_data: the idata for the next object.
   *
   * @pre: idata must contain a key_data.
   * @post: out_data contains complete information
   */
  int prev(const index_data &idata, index_data * out_data);

  /**
   * finds the index_data where a key belongs, from cache if possible. If it
   * reads the index object, it will read the first cache_size entries after
   * key and put them in the cache.
   *
   * @param key: the key to search for
   * @param idata: the index_data for the first index value such that idata.key
   * is greater than key.
   * @param next_idata: if not NULL, this will be set to the idata after idata
   * @param force_update: if false, will try to read from cache first.
   *
   * @pre: key is not encoded
   * @post: idata contains complete information
   * stored
   */
  int read_index(const string &key, index_data * idata,
      index_data * next_idata, bool force_update);

  /**
   * Reads obj and generates information about it. Iff the object has >= 2k
   * entries, reads the whole omap and then splits it.
   *
   * @param idata: index data for the object being split
   * @pre: idata contains a key and an obj
   * @post: idata.obj has been split and icache has been updated
   * @return -EBALANCE if obj does not need to be split, 0 if split successful,
   * error from read_object or perform_ops if there is one.
   */
  int split(const index_data &idata);

  /**
   * reads o1 and the next object after o1 and, if necessary, rebalances them.
   * if hk1 is the highest key in the index, calls rebalance on the next highest
   * key.
   *
   * @param idata: index data for the object being rebalanced
   * @param next_idata: index data for the next object. If blank, will read.
   * @pre: idata contains a key and an obj
   * @post: idata.obj has been rebalanced and icache has been updated
   * @return -EBALANCE if no change needed, -ENOENT if o1 does not exist,
   * -ECANCELED if second object does not exist, otherwise, error from
   * perform_ops
   */
  int rebalance(const index_data &idata1, const index_data &next_idata);

  /**
   * performs an ObjectReadOperation to populate odata
   *
   * @post: odata has all information about obj except for key (which is "")
   */
  int read_object(const string &obj, object_data * odata);

  /**
   * performs a maybe_read_for_balance ObjectOperation so the omap is only
   * read if the object is out of bounds.
   */
  int read_object(const string &obj, rebalance_args * args);

  /**
   * sets up owo to change the index in preparation for a split/merge.
   *
   * @param to_create: vector of object_data to be created.
   * @param to_delete: vector of object_data to be deleted.
   * @param owo: the ObjectWriteOperation to set up
   * @param idata: will be populated by index data for this op.
   * @param err: error code reference to pass to omap_cmp
   * @pre: entries in to_create and to_delete must have keys and names.
   */
  void set_up_prefix_index(
      const vector<object_data> &to_create,
      const vector<object_data> &to_delete,
      librados::ObjectWriteOperation * owo,
      index_data * idata,
      int * err);

  /**
   * sets up all make, mark, restore, and delete ops, as well as the remove
   * prefix op, based on idata.
   *
   * @param create_vector: vector of data about the objects to be created.
   * @pre: entries in create_data must have names and omaps and be in idata
   * order
   * @param delete_vector: vector of data about the objects to be deleted
   * @pre: entries in to_delete must have versions and be in idata order
   * @param ops: the owos to set up. the pair is a pair of op identifiers
   * and names of objects - set_up_ops fills these in.
   * @pre: ops must be the correct size and the ObjectWriteOperation pointers
   * must be valid.
   * @param idata: the idata with information about how to set up the ops
   * @pre: idata has valid to_create and to_delete
   * @param err: the int to get the error value for omap_cmp
   */
  void set_up_ops(
      const vector<object_data> &create_vector,
      const vector<object_data> &delete_vector,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
      const index_data &idata,
      int * err);

  /**
   * sets up owo to exclusive create, set omap to to_set, and set
   * unwritable to "0"
   */
  void set_up_make_object(
      const map<std::string, bufferlist> &to_set,
      librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert object version and that object version is
   * writable,
   * then mark it unwritable.
   *
   * @param ver: if this is 0, no version is asserted.
   */
  void set_up_unwrite_object(
      const int &ver, librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert that an object is unwritable and then mark it
   * writable
   */
  void set_up_restore_object(
      librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert that the object is unwritable and then remove it
   */
  void set_up_delete_object(
      librados::ObjectWriteOperation *owo);

  /**
   * perform the operations in ops and handles errors.
   *
   * @param debug_prefix: what to print at the beginning of debug output
   * @param idata: the idata for the object being operated on, to be
   * passed to cleanup if necessary
   * @param ops: this contains an int identifying the type of op,
   * a string that is the name of the object to operate on, and a pointer
   * to the ObjectWriteOperation to use. All of this must be complete.
   * @post: all operations are performed and most errors are handled
   * (e.g., cleans up if an assertion fails). If an unknown error is found,
   * returns it.
   */
  int perform_ops( const string &debug_prefix,
      const index_data &idata,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops);

  /**
   * Called when a client discovers that another client has died during  a
   * split or a merge. cleans up after that client.
   *
   * @param idata: the index data parsed from the index entry left by the dead
   * client.
   * @param error: the error that caused the client to realize the other client
   * died (should be -ENOENT or -ETIMEDOUT)
   * @post: rolls forward if -ENOENT, otherwise rolls back.
   */
  int cleanup(const index_data &idata, const int &error);

  /**
   * does the ObjectWriteOperation and splits, reads the index, and/or retries
   * until success.
   */
  int set_op(const string &key, const bufferlist &val,
      bool update_on_existing, index_data &idata);

  /**
   * does the ObjectWriteOperation and merges, reads the index, and/or retries
   * until success.
   */
  int remove_op(const string &key, index_data &idata, index_data &next_idata);

  /**
   * does the ObjectWriteOperation and reads the index and/or retries
   * until success.
   */
  int get_op(const string &key, bufferlist * val, index_data &idata);

  /**
   * does the ObjectWriteOperation and splits, reads the index, and/or retries
   * until success.
   */
  int handle_set_rm_errors(int &err, string key, string obj,
      index_data * idata, index_data * next_idata);

  /**
   * called by aio_set, aio_remove, and aio_get, respectively.
   */
  static void* pset(void *ptr);
  static void* prm(void *ptr);
  static void* pget(void *ptr);
public:


  //interruption methods, for correctness testing
  /**
   * returns 0
   */
  int nothing();
  /**
   * 10% chance of waiting wait_ms seconds
   */
  int wait();
  /**
   * 10% chance of killing the client.
   */
  int suicide();

KvFlatBtreeAsync(int k_val, string name, int cache, double cache_r,
    bool verb)
  : k(k_val),
    index_name("index_object"),
    rados_id(name),
    client_name(string(name).append(".")),
    pool_name("rbd"),
    interrupt(&KeyValueStructure::nothing),
    wait_ms(0),
    timeout(100000,0),
    cache_size(cache),
    cache_refresh(cache_r),
    verbose(verb),
    client_index_lock("client_index_lock"),
    client_index(0),
    icache_lock("icache_lock"),
    icache(cache)
  {}

  /**
   * creates a string with an int at the end.
   *
   * @param s: the string on the left
   * @param i: the int to be appended to the string
   * @return the string
   */
  static string to_string(string s, int i);

  /**
   * returns in encoded
   */
  static bufferlist to_bl(const string &in) {
    bufferlist bl;
    bl.append(in);
    return bl;
  }

  /**
   * returns idata encoded;
   */
  static bufferlist to_bl(const index_data &idata) {
    bufferlist bl;
    idata.encode(bl);
    return bl;
  }

  /**
   * returns the rados_id of this KvFlatBtreeAsync
   */
  string get_name();

  /**
   * sets this kvba to call inject before every ObjectWriteOperation.
   * If inject is wait and wait_time is set, wait will have a 10% chance of
   * sleeping for waite_time miliseconds.
   */
  void set_inject(injection_t inject, int wait_time);

  /**
   * sets up the rados and io_ctx of this KvFlatBtreeAsync. If the don't already
   * exist, creates the index and max object.
   */
  int setup(int argc, const char** argv);

  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int remove(const string &key);

  /**
   * returns true if all of the following are true:
   *
   * all objects are accounted for in the index or a prefix
   * 	(i.e., no floating objects)
   * all objects have k <= size <= 2k
   * all keys in an object are within the specified predicted by the index
   *
   * if any of those fails, states that the problem(s) are, and prints str().
   *
   * @pre: no operations are in progress
   */
  bool is_consistent();

  /**
   * returns an ASCII representation of the index and sub objects, showing
   * stats about each object and all omaps. Don't use if you have more than
   * about 10 objects.
   */
  string str();

  int get(const string &key, bufferlist *val);

  //async versions of these methods
  void aio_get(const string &key, bufferlist *val, callback cb,
      void *cb_args, int * err);
  void aio_set(const string &key, const bufferlist &val, bool exclusive,
      callback cb, void * cb_args, int * err);
  void aio_remove(const string &key, callback cb, void *cb_args, int * err);

  //these methods that deal with multiple keys at once are efficient, but make
  //no guarantees about atomicity!

  /**
   * Removes all objects and resets the store as if setup had just run. Makes no
   * attempt to do this safely - make sure this is the only operation running
   * when it is called!
   */
  int remove_all();

  /**
   * This does not add prefixes to the index and therefore DOES NOT guarantee
   * consistency! It is ONLY safe if there is only one instance at a time.
   * It follows the same general logic as a rebalance, but
   * with all objects that contain any of the keys in in_map. It is O(n), where
   * n is the number of librados objects it has to change. Higher object sizes
   * (i.e., k values) also decrease the efficiency of this method because it
   * copies all of the entries in each object it modifies. Writing new objects
   * is done in parallel.
   *
   * This is efficient if:
   * * other clients are very unlikely to be modifying any of the objects while
   * this operation is in progress
   * * The entries in in_map are close together
   * * It is especially efficient for initially entering lots of entries into
   * an empty structure.
   *
   * It is very inefficient compared to setting one key and/or will starve if:
   * * other clients are modifying the objects it tries to modify
   * * The keys are distributed across the range of keys in the store
   * * there is a small number of keys compared to k
   */
  int set_many(const map<string, bufferlist> &in_map);

  int get_all_keys(std::set<string> *keys);
  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

};

#endif /* KVFLATBTREEASYNC_H_ */
