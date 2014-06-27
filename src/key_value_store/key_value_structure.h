/*
 * Interface for key-value store using librados
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

#ifndef KEY_VALUE_STRUCTURE_HPP_
#define KEY_VALUE_STRUCTURE_HPP_

#include "include/rados/librados.hpp"
#include "include/utime.h"
#include <vector>

using std::string;
using std::map;
using std::set;
using ceph::bufferlist;

class KeyValueStructure;

/**An injection_t is a function that is called before every
 * ObjectWriteOperation to test concurrency issues. For example,
 * one injection_t might cause the client to have a greater chance of dying
 * mid-split/merge.
 */
typedef int (KeyValueStructure::*injection_t)();

/**
 * Passed to aio methods to be called when the operation completes
 */
typedef void (*callback)(int * err, void *arg);

class KeyValueStructure{
public:
  map<char, int> opmap;

  //these are injection methods. By default, nothing is called at each
  //interruption point.
  /**
   * returns 0
   */
  virtual int nothing() = 0;
  /**
   * 10% chance of waiting wait_ms seconds
   */
  virtual int wait() = 0;
  /**
   * 10% chance of killing the client.
   */
  virtual int suicide() = 0;

  ////////////////DESTRUCTOR/////////////////
  virtual ~KeyValueStructure() {}

  ////////////////UPDATERS///////////////////

  /**
   * set up the KeyValueStructure (i.e., initialize rados/io_ctx, etc.)
   */
  virtual int setup(int argc, const char** argv) = 0;

  /**
   * set the method that gets called before each ObjectWriteOperation.
   * If waite_time is set and the method passed involves waiting, it will wait
   * for that many miliseconds.
   */
  virtual void set_inject(injection_t inject, int wait_time) = 0;

  /**
   * if update_on_existing is false, returns an error if
   * key already exists in the structure
   */
  virtual int set(const string &key, const bufferlist &val,
      bool update_on_existing) = 0;

  /**
   * efficiently insert the contents of in_map into the structure
   */
  virtual int set_many(const map<string, bufferlist> &in_map) = 0;

  /**
   * removes the key-value for key. returns an error if key does not exist
   */
  virtual int remove(const string &key) = 0;

  /**
   * removes all keys and values
   */
  virtual int remove_all() = 0;


  /**
   * launches a thread to get the value of key. When complete, calls cb(cb_args)
   */
  virtual void aio_get(const string &key, bufferlist *val, callback cb,
      void *cb_args, int * err) = 0;

  /**
   * launches a thread to set key to val. When complete, calls cb(cb_args)
   */
  virtual void aio_set(const string &key, const bufferlist &val, bool exclusive,
      callback cb, void * cb_args, int * err) = 0;

  /**
   * launches a thread to remove key. When complete, calls cb(cb_args)
   */
  virtual void aio_remove(const string &key, callback cb, void *cb_args,
      int * err) = 0;

  ////////////////READERS////////////////////
  /**
   * gets the val associated with key.
   *
   * @param key the key to get
   * @param val the value is stored in this
   * @return error code
   */
  virtual int get(const string &key, bufferlist *val) = 0;

  /**
   * stores all keys in keys. set should put them in order by key.
   */
  virtual int get_all_keys(std::set<string> *keys) = 0;

  /**
   * stores all keys and values in kv_map. map should put them in order by key.
   */
  virtual int get_all_keys_and_values(map<string,bufferlist> *kv_map) = 0;

  /**
   * True if the structure meets its own requirements for consistency.
   */
  virtual bool is_consistent() = 0;

  /**
   * prints a string representation of the structure
   */
  virtual string str() = 0;
};


#endif /* KEY_VALUE_STRUCTURE_HPP_ */
